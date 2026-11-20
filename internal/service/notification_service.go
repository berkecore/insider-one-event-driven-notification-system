package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/repository"
	"github.com/google/uuid"
)

// StatusPublisher broadcasts real-time notification status transitions (e.g. via Redis Pub/Sub).
type StatusPublisher interface {
	Publish(ctx context.Context, ev domain.StatusEvent) error
}

// CreateNotificationInput captures parameters for creating a single notification.
type CreateNotificationInput struct {
	Recipient      string            `json:"recipient"`
	Channel        domain.Channel    `json:"channel"`
	Content        string            `json:"content"`
	Priority       domain.Priority   `json:"priority"`
	TemplateID     *uuid.UUID        `json:"template_id,omitempty"`
	TemplateVars   map[string]string `json:"template_vars,omitempty"`
	ScheduledAt    *time.Time        `json:"scheduled_at,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}

// CreateNotificationResult wraps the created (or replayed) notification.
type CreateNotificationResult struct {
	Notification *domain.Notification
	Replayed     bool
}

// CreateBatchInput captures a batch notification creation request (up to 1000 items).
type CreateBatchInput struct {
	Items          []CreateNotificationInput
	IdempotencyKey string
}

// CreateBatchResult wraps the created (or replayed) batch summary.
type CreateBatchResult struct {
	Summary  *domain.BatchSummary
	Replayed bool
}

// NotificationService orchestrates validation, idempotency, template rendering, and persistence.
type NotificationService struct {
	notifRepo    repository.NotificationRepository
	templateRepo repository.TemplateRepository
	publisher    StatusPublisher
	metrics      *observability.Metrics
	logger       *slog.Logger
	maxAttempts  int
	clock        func() time.Time
}

// NewNotificationService constructs a NotificationService.
func NewNotificationService(
	notifRepo repository.NotificationRepository,
	templateRepo repository.TemplateRepository,
	publisher StatusPublisher,
	metrics *observability.Metrics,
	logger *slog.Logger,
	maxAttempts int,
) *NotificationService {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &NotificationService{
		notifRepo:    notifRepo,
		templateRepo: templateRepo,
		publisher:    publisher,
		metrics:      metrics,
		logger:       logger,
		maxAttempts:  maxAttempts,
		clock:        time.Now,
	}
}

// Create validates and persists a single notification with transactional outbox and idempotency support.
func (s *NotificationService) Create(ctx context.Context, input CreateNotificationInput) (*CreateNotificationResult, error) {
	ctx, span := observability.Tracer().Start(ctx, "NotificationService.Create")
	defer span.End()

	reqHash := hashPayload(input)
	idemKey := strings.TrimSpace(input.IdempotencyKey)

	if idemKey != "" {
		existing, err := s.notifRepo.GetByIdempotencyKey(ctx, idemKey)
		if err == nil {
			if existing.RequestHash != "" && existing.RequestHash != reqHash {
				return nil, domain.ErrIdempotencyConflict
			}
			return &CreateNotificationResult{Notification: existing, Replayed: true}, nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("check idempotency key: %w", err)
		}
	}

	tplCache := make(map[uuid.UUID]*domain.Template)
	n, verr, err := s.prepareNotification(ctx, input, nil, reqHash, -1, tplCache)
	if err != nil {
		return nil, err
	}
	if verr != nil && verr.HasErrors() {
		return nil, verr
	}

	if err := s.notifRepo.CreateWithOutbox(ctx, n); err != nil {
		if errors.Is(err, domain.ErrConflict) && idemKey != "" {
			existing, getErr := s.notifRepo.GetByIdempotencyKey(ctx, idemKey)
			if getErr == nil {
				if existing.RequestHash != "" && existing.RequestHash != reqHash {
					return nil, domain.ErrIdempotencyConflict
				}
				return &CreateNotificationResult{Notification: existing, Replayed: true}, nil
			}
		}
		return nil, err
	}

	if s.metrics != nil {
		s.metrics.IncCreated(string(n.Channel), string(n.Priority))
	}
	s.publishStatus(ctx, n)

	observability.LoggerWithContext(ctx, s.logger).Info("notification created",
		slog.String("notification_id", n.ID.String()),
		slog.String("channel", string(n.Channel)),
		slog.String("priority", string(n.Priority)),
		slog.String("status", string(n.Status)),
	)

	return &CreateNotificationResult{Notification: n, Replayed: false}, nil
}

// CreateBatch validates up to 1000 notifications upfront and persists them in one atomic transaction.
func (s *NotificationService) CreateBatch(ctx context.Context, input CreateBatchInput) (*CreateBatchResult, error) {
	ctx, span := observability.Tracer().Start(ctx, "NotificationService.CreateBatch")
	defer span.End()

	if len(input.Items) == 0 {
		return nil, domain.ErrBatchEmpty
	}
	if len(input.Items) > domain.MaxBatchSize {
		return nil, domain.ErrBatchTooLarge
	}

	reqHash := hashPayload(input.Items)
	batchIdemKey := strings.TrimSpace(input.IdempotencyKey)

	if batchIdemKey != "" {
		existing, err := s.notifRepo.GetBatchByIdempotencyKey(ctx, batchIdemKey)
		if err == nil {
			if existing.Batch.RequestHash != "" && existing.Batch.RequestHash != reqHash {
				return nil, domain.ErrIdempotencyConflict
			}
			return &CreateBatchResult{Summary: existing, Replayed: true}, nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("check batch idempotency key: %w", err)
		}
	}

	batchID := uuid.New()
	var batchIdemPtr *string
	if batchIdemKey != "" {
		batchIdemPtr = &batchIdemKey
	}
	batch := &domain.NotificationBatch{
		ID:             batchID,
		IdempotencyKey: batchIdemPtr,
		RequestHash:    reqHash,
		TotalCount:     len(input.Items),
	}

	aggErr := &domain.ValidationError{}
	seenItemKeys := make(map[string]struct{})
	tplCache := make(map[uuid.UUID]*domain.Template)
	items := make([]*domain.Notification, 0, len(input.Items))

	for i, item := range input.Items {
		if k := strings.TrimSpace(item.IdempotencyKey); k != "" {
			if _, dup := seenItemKeys[k]; dup {
				aggErr.AddAt(i, "idempotency_key", "duplicate idempotency_key within batch")
				continue
			}
			seenItemKeys[k] = struct{}{}
		}

		itemHash := hashPayload(item)
		n, verr, err := s.prepareNotification(ctx, item, &batchID, itemHash, i, tplCache)
		if err != nil {
			return nil, err
		}
		if verr != nil && verr.HasErrors() {
			aggErr.Fields = append(aggErr.Fields, verr.Fields...)
			continue
		}
		items = append(items, n)
	}

	if aggErr.HasErrors() {
		return nil, aggErr
	}

	if err := s.notifRepo.CreateBatchWithOutbox(ctx, batch, items); err != nil {
		if errors.Is(err, domain.ErrConflict) && batchIdemKey != "" {
			existing, getErr := s.notifRepo.GetBatchByIdempotencyKey(ctx, batchIdemKey)
			if getErr == nil {
				if existing.Batch.RequestHash != "" && existing.Batch.RequestHash != reqHash {
					return nil, domain.ErrIdempotencyConflict
				}
				return &CreateBatchResult{Summary: existing, Replayed: true}, nil
			}
		}
		return nil, err
	}

	counts := make(map[domain.Status]int)
	notifs := make([]domain.Notification, len(items))
	for i, n := range items {
		counts[n.Status]++
		notifs[i] = *n
		if s.metrics != nil {
			s.metrics.IncCreated(string(n.Channel), string(n.Priority))
		}
		s.publishStatus(ctx, n)
	}

	observability.LoggerWithContext(ctx, s.logger).Info("notification batch created",
		slog.String("batch_id", batch.ID.String()),
		slog.Int("total_count", batch.TotalCount),
	)

	return &CreateBatchResult{
		Summary: &domain.BatchSummary{
			Batch:         *batch,
			StatusCounts:  counts,
			Notifications: notifs,
		},
		Replayed: false,
	}, nil
}

func (s *NotificationService) prepareNotification(
	ctx context.Context,
	input CreateNotificationInput,
	batchID *uuid.UUID,
	reqHash string,
	batchIndex int,
	tplCache map[uuid.UUID]*domain.Template,
) (*domain.Notification, *domain.ValidationError, error) {
	verr := &domain.ValidationError{}
	addErr := func(field, msg string) {
		if batchIndex >= 0 {
			verr.AddAt(batchIndex, field, msg)
		} else {
			verr.Add(field, msg)
		}
	}

	channel := domain.Channel(strings.ToLower(strings.TrimSpace(string(input.Channel))))
	priority := domain.Priority(strings.ToLower(strings.TrimSpace(string(input.Priority))))
	if priority == "" {
		priority = domain.PriorityNormal
	}
	content := input.Content

	if input.TemplateID != nil {
		if s.templateRepo == nil {
			addErr("template_id", "template repository is not configured")
			return nil, verr, nil
		}
		tpl, ok := tplCache[*input.TemplateID]
		if !ok {
			var err error
			tpl, err = s.templateRepo.GetByID(ctx, *input.TemplateID)
			if err != nil {
				if errors.Is(err, domain.ErrTemplateNotFound) {
					addErr("template_id", fmt.Sprintf("template %s not found", input.TemplateID.String()))
					return nil, verr, nil
				}
				return nil, nil, err
			}
			tplCache[*input.TemplateID] = tpl
		}
		if channel == "" {
			channel = tpl.Channel
		} else if channel != tpl.Channel {
			addErr("channel", fmt.Sprintf("must match template channel %q", tpl.Channel))
			return nil, verr, nil
		}
		rendered, renderErr := tpl.Render(input.TemplateVars)
		if renderErr != nil {
			var rv *domain.ValidationError
			if errors.As(renderErr, &rv) {
				for _, f := range rv.Fields {
					addErr(f.Field, f.Message)
				}
				return nil, verr, nil
			}
			return nil, nil, renderErr
		}
		content = rendered
	}

	if fieldErrs := domain.ValidateNotificationFields(input.Recipient, channel, content, priority); fieldErrs != nil {
		for _, f := range fieldErrs.Fields {
			addErr(f.Field, f.Message)
		}
	}

	now := s.clock().UTC()
	status := domain.StatusPending
	var scheduledAt *time.Time
	if input.ScheduledAt != nil {
		t := input.ScheduledAt.UTC()
		if t.After(now) {
			status = domain.StatusScheduled
			scheduledAt = &t
		}
	}

	if verr.HasErrors() {
		return nil, verr, nil
	}

	var idemPtr *string
	if k := strings.TrimSpace(input.IdempotencyKey); k != "" {
		idemPtr = &k
	}

	n := &domain.Notification{
		ID:             uuid.New(),
		BatchID:        batchID,
		IdempotencyKey: idemPtr,
		RequestHash:    reqHash,
		Recipient:      strings.TrimSpace(input.Recipient),
		Channel:        channel,
		Content:        content,
		Priority:       priority,
		Status:         status,
		TemplateID:     input.TemplateID,
		CorrelationID:  observability.CorrelationIDFromContext(ctx),
		TraceContext:   observability.InjectTraceContext(ctx),
		ScheduledAt:    scheduledAt,
		MaxAttempts:    s.maxAttempts,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	return n, nil, nil
}

// GetByID retrieves a single notification and its delivery attempt history.
func (s *NotificationService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return s.notifRepo.GetByID(ctx, id)
}

// GetBatchByID retrieves a batch summary including per-status counts and all items.
func (s *NotificationService) GetBatchByID(ctx context.Context, batchID uuid.UUID) (*domain.BatchSummary, error) {
	return s.notifRepo.GetBatchByID(ctx, batchID)
}

// List returns filtered, paginated notifications.
func (s *NotificationService) List(ctx context.Context, filter domain.NotificationFilter) (*domain.NotificationPage, error) {
	return s.notifRepo.List(ctx, filter)
}

// Cancel atomically cancels a notification if it has not yet started final delivery.
func (s *NotificationService) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := s.notifRepo.Cancel(ctx, id)
	if err != nil {
		return nil, err
	}
	if s.metrics != nil {
		s.metrics.IncCancelled(string(n.Channel))
	}
	s.publishStatus(ctx, n)
	observability.LoggerWithContext(ctx, s.logger).Info("notification cancelled",
		slog.String("notification_id", n.ID.String()),
		slog.String("channel", string(n.Channel)),
	)
	return n, nil
}

// Replay re-enqueues a permanently failed notification for fresh delivery.
func (s *NotificationService) Replay(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := s.notifRepo.Replay(ctx, id)
	if err != nil {
		return nil, err
	}
	s.publishStatus(ctx, n)
	observability.LoggerWithContext(ctx, s.logger).Info("notification replayed from DLQ",
		slog.String("notification_id", n.ID.String()),
		slog.String("channel", string(n.Channel)),
	)
	return n, nil
}

func (s *NotificationService) publishStatus(ctx context.Context, n *domain.Notification) {
	if s.publisher == nil || n == nil {
		return
	}
	ev := domain.StatusEvent{
		NotificationID:    n.ID,
		BatchID:           n.BatchID,
		Channel:           n.Channel,
		Priority:          n.Priority,
		Status:            n.Status,
		AttemptCount:      n.AttemptCount,
		ProviderMessageID: n.ProviderMessageID,
		LastError:         n.LastError,
		CorrelationID:     n.CorrelationID,
		Timestamp:         s.clock().UTC(),
	}
	if err := s.publisher.Publish(ctx, ev); err != nil {
		observability.LoggerWithContext(ctx, s.logger).Warn("failed to publish status event",
			slog.String("notification_id", n.ID.String()),
			slog.Any("error", err),
		)
	}
}

func hashPayload(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/google/uuid"
)

type memNotifRepo struct {
	mu           sync.Mutex
	byID         map[uuid.UUID]*domain.Notification
	byIdem       map[string]*domain.Notification
	batches      map[uuid.UUID]*domain.BatchSummary
	batchByIdem  map[string]*domain.BatchSummary
}

func newMemNotifRepo() *memNotifRepo {
	return &memNotifRepo{
		byID:        make(map[uuid.UUID]*domain.Notification),
		byIdem:      make(map[string]*domain.Notification),
		batches:     make(map[uuid.UUID]*domain.BatchSummary),
		batchByIdem: make(map[string]*domain.BatchSummary),
	}
}

func (m *memNotifRepo) CreateWithOutbox(_ context.Context, n *domain.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n.IdempotencyKey != nil {
		if _, exists := m.byIdem[*n.IdempotencyKey]; exists {
			return domain.ErrConflict
		}
		m.byIdem[*n.IdempotencyKey] = n
	}
	cp := *n
	m.byID[n.ID] = &cp
	return nil
}

func (m *memNotifRepo) CreateBatchWithOutbox(_ context.Context, batch *domain.NotificationBatch, items []*domain.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if batch.IdempotencyKey != nil {
		if _, exists := m.batchByIdem[*batch.IdempotencyKey]; exists {
			return domain.ErrConflict
		}
	}
	notifs := make([]domain.Notification, len(items))
	counts := make(map[domain.Status]int)
	for i, n := range items {
		cp := *n
		m.byID[n.ID] = &cp
		notifs[i] = cp
		counts[n.Status]++
	}
	summary := &domain.BatchSummary{
		Batch:         *batch,
		StatusCounts:  counts,
		Notifications: notifs,
	}
	m.batches[batch.ID] = summary
	if batch.IdempotencyKey != nil {
		m.batchByIdem[*batch.IdempotencyKey] = summary
	}
	return nil
}

func (m *memNotifRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *n
	return &cp, nil
}

func (m *memNotifRepo) GetByIdempotencyKey(_ context.Context, key string) (*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.byIdem[key]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *n
	return &cp, nil
}

func (m *memNotifRepo) GetBatchByID(_ context.Context, batchID uuid.UUID) (*domain.BatchSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.batches[batchID]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return b, nil
}

func (m *memNotifRepo) GetBatchByIdempotencyKey(_ context.Context, key string) (*domain.BatchSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.batchByIdem[key]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return b, nil
}

func (m *memNotifRepo) List(_ context.Context, _ domain.NotificationFilter) (*domain.NotificationPage, error) {
	return &domain.NotificationPage{Items: []domain.Notification{}, Page: 1, PageSize: 20}, nil
}

func (m *memNotifRepo) Cancel(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if !n.Status.CanCancel() {
		return nil, domain.ErrNotCancellable
	}
	n.Status = domain.StatusCancelled
	cp := *n
	return &cp, nil
}

func (m *memNotifRepo) Replay(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if n.Status != domain.StatusFailed {
		return nil, domain.ErrNotReplayable
	}
	n.Status = domain.StatusPending
	cp := *n
	return &cp, nil
}

func (m *memNotifRepo) ClaimForProcessing(_ context.Context, _ uuid.UUID) (*domain.Notification, error) {
	return nil, nil
}
func (m *memNotifRepo) MarkDelivered(_ context.Context, _ uuid.UUID, _ string) (*domain.Notification, error) {
	return nil, nil
}
func (m *memNotifRepo) MarkRetrying(_ context.Context, _ uuid.UUID, _ time.Time, _ string) (*domain.Notification, error) {
	return nil, nil
}
func (m *memNotifRepo) MarkDeferred(_ context.Context, _ uuid.UUID, _ time.Time, _ string) (*domain.Notification, error) {
	return nil, nil
}
func (m *memNotifRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) (*domain.Notification, error) {
	return nil, nil
}
func (m *memNotifRepo) PromoteScheduled(_ context.Context, _ int) ([]domain.Notification, error) {
	return nil, nil
}
func (m *memNotifRepo) ReclaimStaleProcessing(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}

type memTplRepo struct {
	items map[uuid.UUID]*domain.Template
}

func (m *memTplRepo) Create(_ context.Context, tpl *domain.Template) error {
	m.items[tpl.ID] = tpl
	return nil
}
func (m *memTplRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Template, error) {
	t, ok := m.items[id]
	if !ok {
		return nil, domain.ErrTemplateNotFound
	}
	return t, nil
}
func (m *memTplRepo) List(_ context.Context, _ *domain.Channel) ([]domain.Template, error) {
	return nil, nil
}
func (m *memTplRepo) Update(_ context.Context, _ *domain.Template) error { return nil }
func (m *memTplRepo) Delete(_ context.Context, _ uuid.UUID) error        { return nil }

func TestCreateIdempotencyAndTemplateRendering(t *testing.T) {
	notifRepo := newMemNotifRepo()
	tplRepo := &memTplRepo{items: make(map[uuid.UUID]*domain.Template)}
	tplID := uuid.New()
	tplRepo.items[tplID] = &domain.Template{
		ID:      tplID,
		Name:    "otp",
		Channel: domain.ChannelSMS,
		Body:    "Your OTP is {{code}}",
	}

	svc := NewNotificationService(notifRepo, tplRepo, nil, observability.NewMetrics(), nil, 5)
	ctx := observability.WithCorrelationID(context.Background(), "corr-1")

	res1, err := svc.Create(ctx, CreateNotificationInput{
		Recipient:      "+905551234567",
		Channel:        domain.ChannelSMS,
		TemplateID:     &tplID,
		TemplateVars:   map[string]string{"code": "987654"},
		IdempotencyKey: "key-abc",
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if res1.Replayed || res1.Notification.Content != "Your OTP is 987654" {
		t.Fatalf("unexpected create result: %+v", res1)
	}

	// Same idempotency key + same payload => replay
	res2, err := svc.Create(ctx, CreateNotificationInput{
		Recipient:      "+905551234567",
		Channel:        domain.ChannelSMS,
		TemplateID:     &tplID,
		TemplateVars:   map[string]string{"code": "987654"},
		IdempotencyKey: "key-abc",
	})
	if err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
	if !res2.Replayed || res2.Notification.ID != res1.Notification.ID {
		t.Fatalf("expected replayed notification %s, got %+v", res1.Notification.ID, res2)
	}

	// Same idempotency key + different payload => ErrIdempotencyConflict
	_, err = svc.Create(ctx, CreateNotificationInput{
		Recipient:      "+905551234567",
		Channel:        domain.ChannelSMS,
		TemplateID:     &tplID,
		TemplateVars:   map[string]string{"code": "000000"},
		IdempotencyKey: "key-abc",
	})
	if !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

func TestBatchLimitsScheduledAndCancel(t *testing.T) {
	notifRepo := newMemNotifRepo()
	svc := NewNotificationService(notifRepo, nil, nil, observability.NewMetrics(), nil, 5)

	// Empty batch rejected
	if _, err := svc.CreateBatch(context.Background(), CreateBatchInput{}); !errors.Is(err, domain.ErrBatchEmpty) {
		t.Fatalf("expected ErrBatchEmpty, got %v", err)
	}

	// >1000 batch rejected
	oversized := make([]CreateNotificationInput, domain.MaxBatchSize+1)
	if _, err := svc.CreateBatch(context.Background(), CreateBatchInput{Items: oversized}); !errors.Is(err, domain.ErrBatchTooLarge) {
		t.Fatalf("expected ErrBatchTooLarge, got %v", err)
	}

	// Scheduled notification gets StatusScheduled and can be cancelled
	future := time.Now().Add(1 * time.Hour)
	res, err := svc.Create(context.Background(), CreateNotificationInput{
		Recipient:   "user@example.com",
		Channel:     domain.ChannelEmail,
		Content:     "Future newsletter",
		ScheduledAt: &future,
	})
	if err != nil {
		t.Fatalf("scheduled create failed: %v", err)
	}
	if res.Notification.Status != domain.StatusScheduled {
		t.Fatalf("expected scheduled status, got %s", res.Notification.Status)
	}

	cancelled, err := svc.Cancel(context.Background(), res.Notification.ID)
	if err != nil || cancelled.Status != domain.StatusCancelled {
		t.Fatalf("expected cancelled status, got %+v err=%v", cancelled, err)
	}

	// Cancelling already cancelled returns ErrNotCancellable
	if _, err := svc.Cancel(context.Background(), res.Notification.ID); !errors.Is(err, domain.ErrNotCancellable) {
		t.Fatalf("expected ErrNotCancellable, got %v", err)
	}
}

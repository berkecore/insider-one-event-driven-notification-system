package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/provider"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/queue"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/repository"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"go.opentelemetry.io/otel/attribute"
)

// Processor executes the end-to-end delivery pipeline for a single dequeued notification.
type Processor struct {
	notifRepo   repository.NotificationRepository
	attemptRepo repository.AttemptRepository
	queue       queue.Queue
	rateLimiter queue.RateLimiter
	provider    provider.Provider
	publisher   service.StatusPublisher
	metrics     *observability.Metrics
	backoff     BackoffPolicy
	logger      *slog.Logger
	clock       func() time.Time
}

func NewProcessor(
	notifRepo repository.NotificationRepository,
	attemptRepo repository.AttemptRepository,
	q queue.Queue,
	rateLimiter queue.RateLimiter,
	prov provider.Provider,
	publisher service.StatusPublisher,
	metrics *observability.Metrics,
	backoff BackoffPolicy,
	logger *slog.Logger,
) *Processor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Processor{
		notifRepo:   notifRepo,
		attemptRepo: attemptRepo,
		queue:       q,
		rateLimiter: rateLimiter,
		provider:    prov,
		publisher:   publisher,
		metrics:     metrics,
		backoff:     backoff,
		logger:      logger,
		clock:       time.Now,
	}
}

// Process claims a notification atomically, enforces rate limiting, delivers through the
// circuit-breaker-protected provider, records the attempt, and handles retries or DLQ routing.
func (p *Processor) Process(ctx context.Context, msg queue.Message) error {
	ctx = observability.ExtractTraceContext(ctx, msg.TraceContext)
	if msg.CorrelationID != "" {
		ctx = observability.WithCorrelationID(ctx, msg.CorrelationID)
	}

	ctx, span := observability.Tracer().Start(ctx, "worker.Process")
	defer span.End()
	span.SetAttributes(
		attribute.String("notification.id", msg.NotificationID.String()),
		attribute.String("notification.channel", string(msg.Channel)),
		attribute.String("notification.priority", string(msg.Priority)),
	)

	log := observability.LoggerWithContext(ctx, p.logger).With(
		slog.String("notification_id", msg.NotificationID.String()),
		slog.String("channel", string(msg.Channel)),
	)

	// 1. Atomically claim the notification. Skips cancelled, already delivered, or in-flight items.
	n, err := p.notifRepo.ClaimForProcessing(ctx, msg.NotificationID)
	if err != nil {
		return err
	}
	if n == nil {
		log.Debug("skipping notification: already cancelled, delivered, or claimed")
		return nil
	}

	p.publishStatus(ctx, n)

	// 2. Enforce distributed per-channel rate limit (max 100 msg/s/channel).
	if p.rateLimiter != nil {
		if err := p.rateLimiter.Wait(ctx, n.Channel); err != nil {
			// Graceful shutdown while waiting for a rate limit token: defer back to retry ZSET.
			deferCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			nextAt := p.clock().Add(250 * time.Millisecond)
			_, _ = p.notifRepo.MarkDeferred(deferCtx, n.ID, nextAt, "deferred during worker shutdown")
			_ = p.queue.ScheduleRetry(deferCtx, msg, nextAt)
			return err
		}
	}

	// 3. Deliver via external provider (wrapped in per-channel circuit breaker).
	start := p.clock()
	resp, delivErr := p.provider.Deliver(ctx, provider.DeliveryRequest{
		To:             n.Recipient,
		Channel:        string(n.Channel),
		Content:        n.Content,
		NotificationID: n.ID.String(),
		CorrelationID:  n.CorrelationID,
		AttemptNumber:  n.AttemptCount,
	})
	elapsed := p.clock().Sub(start)
	if elapsed < 0 {
		elapsed = 0
	}

	// 4a. Circuit breaker open -> defer cleanly without burning an attempt.
	if cbErr, ok := provider.IsCircuitOpen(delivErr); ok {
		delay := cbErr.RetryAfter
		if delay < time.Second {
			delay = time.Second
		}
		nextAt := p.clock().Add(delay)
		updated, err := p.notifRepo.MarkDeferred(ctx, n.ID, nextAt, cbErr.Error())
		if err != nil {
			return err
		}
		if err := p.queue.ScheduleRetry(ctx, msg, nextAt); err != nil {
			return err
		}
		if p.metrics != nil {
			p.metrics.RecordRetried(string(n.Channel), 0)
		}
		p.publishStatus(ctx, updated)
		log.Warn("delivery deferred by open circuit breaker", slog.Duration("retry_after", delay))
		return nil
	}

	// 4b. Delivery succeeded!
	if delivErr == nil {
		msgID := resp.MessageID
		p.recordAttempt(ctx, &domain.DeliveryAttempt{
			NotificationID:    n.ID,
			AttemptNumber:     n.AttemptCount,
			StatusCode:        resp.StatusCode,
			ProviderMessageID: &msgID,
			Retryable:         false,
			DurationMs:        elapsed.Milliseconds(),
		})

		updated, err := p.notifRepo.MarkDelivered(ctx, n.ID, msgID)
		if err != nil {
			return err
		}
		if p.metrics != nil {
			p.metrics.RecordDelivered(string(n.Channel), elapsed)
		}
		p.publishStatus(ctx, updated)
		log.Info("notification delivered",
			slog.String("provider_message_id", msgID),
			slog.Int("attempt", n.AttemptCount),
			slog.Int64("duration_ms", elapsed.Milliseconds()),
		)
		return nil
	}

	// 4c. Delivery failed: classify retryable vs permanent error.
	var (
		statusCode int
		retryable  = true
		retryAfter time.Duration
	)
	var derr *provider.DeliveryError
	if errors.As(delivErr, &derr) {
		statusCode = derr.StatusCode
		retryable = derr.Retryable
		retryAfter = derr.RetryAfter
	}

	errMsg := delivErr.Error()
	p.recordAttempt(ctx, &domain.DeliveryAttempt{
		NotificationID: n.ID,
		AttemptNumber:  n.AttemptCount,
		StatusCode:     statusCode,
		ErrorMessage:   &errMsg,
		Retryable:      retryable,
		DurationMs:     elapsed.Milliseconds(),
	})

	if retryable && n.AttemptCount < n.MaxAttempts {
		delay := p.backoff.NextDelay(n.AttemptCount, retryAfter)
		nextAt := p.clock().Add(delay)
		updated, err := p.notifRepo.MarkRetrying(ctx, n.ID, nextAt, errMsg)
		if err != nil {
			return err
		}
		if err := p.queue.ScheduleRetry(ctx, msg, nextAt); err != nil {
			return err
		}
		if p.metrics != nil {
			p.metrics.RecordRetried(string(n.Channel), elapsed)
		}
		p.publishStatus(ctx, updated)
		log.Warn("notification delivery failed; scheduled retry",
			slog.Int("attempt", n.AttemptCount),
			slog.Int("max_attempts", n.MaxAttempts),
			slog.Duration("backoff", delay),
			slog.String("error", errMsg),
		)
		return nil
	}

	// Permanent error or retries exhausted -> mark failed and route to DLQ.
	updated, err := p.notifRepo.MarkFailed(ctx, n.ID, errMsg)
	if err != nil {
		return err
	}
	if err := p.queue.PushDLQ(ctx, msg, errMsg); err != nil {
		log.Error("failed to push notification to DLQ", slog.Any("error", err))
	}
	if p.metrics != nil {
		p.metrics.RecordFailed(string(n.Channel), elapsed)
	}
	p.publishStatus(ctx, updated)
	log.Error("notification permanently failed and routed to DLQ",
		slog.Int("attempt", n.AttemptCount),
		slog.Bool("retryable", retryable),
		slog.String("error", errMsg),
	)
	return nil
}

func (p *Processor) recordAttempt(ctx context.Context, a *domain.DeliveryAttempt) {
	if p.attemptRepo == nil {
		return
	}
	if err := p.attemptRepo.Record(ctx, a); err != nil {
		observability.LoggerWithContext(ctx, p.logger).Warn("failed to record delivery attempt",
			slog.String("notification_id", a.NotificationID.String()),
			slog.Any("error", err),
		)
	}
}

func (p *Processor) publishStatus(ctx context.Context, n *domain.Notification) {
	if p.publisher == nil || n == nil {
		return
	}
	_ = p.publisher.Publish(ctx, domain.StatusEvent{
		NotificationID:    n.ID,
		BatchID:           n.BatchID,
		Channel:           n.Channel,
		Priority:          n.Priority,
		Status:            n.Status,
		AttemptCount:      n.AttemptCount,
		ProviderMessageID: n.ProviderMessageID,
		LastError:         n.LastError,
		CorrelationID:     n.CorrelationID,
		Timestamp:         p.clock().UTC(),
	})
}

package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/queue"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/repository"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
)

// OutboxRelay reliably transfers committed PostgreSQL outbox events into Redis priority queues.
type OutboxRelay struct {
	outboxRepo   repository.OutboxRepository
	queue        queue.Queue
	publisher    service.StatusPublisher
	logger       *slog.Logger
	pollInterval time.Duration
	batchSize    int
}

func NewOutboxRelay(
	outboxRepo repository.OutboxRepository,
	q queue.Queue,
	publisher service.StatusPublisher,
	logger *slog.Logger,
	pollInterval time.Duration,
	batchSize int,
) *OutboxRelay {
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OutboxRelay{
		outboxRepo:   outboxRepo,
		queue:        q,
		publisher:    publisher,
		logger:       logger,
		pollInterval: pollInterval,
		batchSize:    batchSize,
	}
}

// Run polls the outbox table until ctx is cancelled.
func (r *OutboxRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		count, err := r.relayOnce(ctx)
		if err != nil && ctx.Err() == nil {
			r.logger.Error("outbox relay batch failed", slog.Any("error", err))
		}
		// If a full batch was drained, loop immediately to clear burst backlogs quickly.
		if count >= r.batchSize && ctx.Err() == nil {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *OutboxRelay) relayOnce(ctx context.Context) (int, error) {
	relayed, err := r.outboxRepo.ProcessPending(ctx, r.batchSize, func(events []domain.OutboxEvent) error {
		msgs := make([]queue.Message, len(events))
		now := time.Now().UTC()
		for i, ev := range events {
			msgs[i] = queue.Message{
				NotificationID: ev.NotificationID,
				Channel:        ev.Channel,
				Priority:       ev.Priority,
				CorrelationID:  ev.CorrelationID,
				TraceContext:   ev.TraceContext,
				EnqueuedAt:     now,
			}
		}
		return r.queue.EnqueueBatch(ctx, msgs)
	})
	if err != nil {
		return 0, err
	}
	if len(relayed) == 0 {
		return 0, nil
	}

	if r.publisher != nil {
		now := time.Now().UTC()
		for _, ev := range relayed {
			_ = r.publisher.Publish(ctx, domain.StatusEvent{
				NotificationID: ev.NotificationID,
				Channel:        ev.Channel,
				Priority:       ev.Priority,
				Status:         domain.StatusQueued,
				CorrelationID:  ev.CorrelationID,
				Timestamp:      now,
			})
		}
	}
	return len(relayed), nil
}

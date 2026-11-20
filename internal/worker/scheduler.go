package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/repository"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
)

// Scheduler promotes due scheduled notifications and reclaims stale in-flight processing leases.
type Scheduler struct {
	notifRepo     repository.NotificationRepository
	publisher     service.StatusPublisher
	logger        *slog.Logger
	pollInterval  time.Duration
	leaseDuration time.Duration
}

func NewScheduler(
	notifRepo repository.NotificationRepository,
	publisher service.StatusPublisher,
	logger *slog.Logger,
	pollInterval time.Duration,
	leaseDuration time.Duration,
) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	if leaseDuration <= 0 {
		leaseDuration = 60 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		notifRepo:     notifRepo,
		publisher:     publisher,
		logger:        logger,
		pollInterval:  pollInterval,
		leaseDuration: leaseDuration,
	}
}

// Run loops until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	schedTicker := time.NewTicker(s.pollInterval)
	reclaimTicker := time.NewTicker(10 * time.Second)
	defer schedTicker.Stop()
	defer reclaimTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-schedTicker.C:
			promoted, err := s.notifRepo.PromoteScheduled(ctx, 100)
			if err != nil && ctx.Err() == nil {
				s.logger.Error("promote scheduled notifications failed", slog.Any("error", err))
				continue
			}
			if len(promoted) > 0 {
				s.logger.Info("promoted scheduled notifications", slog.Int("count", len(promoted)))
				if s.publisher != nil {
					now := time.Now().UTC()
					for _, n := range promoted {
						_ = s.publisher.Publish(ctx, domain.StatusEvent{
							NotificationID: n.ID,
							BatchID:        n.BatchID,
							Channel:        n.Channel,
							Priority:       n.Priority,
							Status:         domain.StatusPending,
							CorrelationID:  n.CorrelationID,
							Timestamp:      now,
						})
					}
				}
			}
		case <-reclaimTicker.C:
			reclaimed, err := s.notifRepo.ReclaimStaleProcessing(ctx, s.leaseDuration, 50)
			if err != nil && ctx.Err() == nil {
				s.logger.Error("reclaim stale processing notifications failed", slog.Any("error", err))
				continue
			}
			if reclaimed > 0 {
				s.logger.Warn("reclaimed stale processing notifications", slog.Int64("count", reclaimed))
			}
		}
	}
}

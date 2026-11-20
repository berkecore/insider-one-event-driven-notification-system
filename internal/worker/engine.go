package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/config"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/queue"
)

// Engine coordinates per-channel worker pools, outbox relay, retry promotion, and scheduling.
type Engine struct {
	cfg       config.WorkerConfig
	queue     queue.Queue
	processor *Processor
	outbox    *OutboxRelay
	scheduler *Scheduler
	metrics   *observability.Metrics
	logger    *slog.Logger

	wg sync.WaitGroup
}

func NewEngine(
	cfg config.WorkerConfig,
	q queue.Queue,
	processor *Processor,
	outbox *OutboxRelay,
	scheduler *Scheduler,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		cfg:       cfg,
		queue:     q,
		processor: processor,
		outbox:    outbox,
		scheduler: scheduler,
		metrics:   metrics,
		logger:    logger,
	}
}

// Start launches all background processing goroutines until ctx is cancelled.
func (e *Engine) Start(ctx context.Context) {
	if e.outbox != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.outbox.Run(ctx)
		}()
	}

	if e.scheduler != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.scheduler.Run(ctx)
		}()
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.runRetryPromoter(ctx)
	}()

	if e.metrics != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.runQueueMetricsScraper(ctx)
		}()
	}

	workers := e.cfg.WorkersPerChannel
	if workers <= 0 {
		workers = 4
	}
	for _, ch := range domain.AllChannels {
		for i := 0; i < workers; i++ {
			channel := ch
			workerNum := i + 1
			e.wg.Add(1)
			go func() {
				defer e.wg.Done()
				e.runChannelWorker(ctx, channel, workerNum)
			}()
		}
	}

	e.logger.Info("worker engine started",
		slog.Int("workers_per_channel", workers),
		slog.Int("total_channel_workers", workers*len(domain.AllChannels)),
	)
}

// Wait blocks until all workers have drained in-flight deliveries and exited.
func (e *Engine) Wait() {
	e.wg.Wait()
	e.logger.Info("worker engine stopped cleanly")
}

func (e *Engine) runChannelWorker(ctx context.Context, channel domain.Channel, workerNum int) {
	const idleSleep = 25 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := e.queue.Dequeue(ctx, channel)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.logger.Error("queue dequeue error",
				slog.String("channel", string(channel)),
				slog.Int("worker", workerNum),
				slog.Any("error", err),
			)
			sleepCtx(ctx, 200*time.Millisecond)
			continue
		}

		if msg == nil {
			sleepCtx(ctx, idleSleep)
			continue
		}

		// Allow in-flight delivery a bounded drain window even if SIGTERM arrives mid-call.
		procCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		if err := e.processor.Process(procCtx, *msg); err != nil && ctx.Err() == nil {
			e.logger.Error("notification processing error",
				slog.String("channel", string(channel)),
				slog.String("notification_id", msg.NotificationID.String()),
				slog.Any("error", err),
			)
		}
		cancel()
	}
}

func (e *Engine) runRetryPromoter(ctx context.Context) {
	interval := e.cfg.RetryPromoterInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, ch := range domain.AllChannels {
				moved, err := e.queue.PromoteDueRetries(ctx, ch, 100)
				if err != nil && ctx.Err() == nil {
					e.logger.Error("retry promoter error",
						slog.String("channel", string(ch)),
						slog.Any("error", err),
					)
					continue
				}
				if moved > 0 {
					e.logger.Debug("promoted due retries",
						slog.String("channel", string(ch)),
						slog.Int64("count", moved),
					)
				}
			}
		}
	}
}

func (e *Engine) runQueueMetricsScraper(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depths, err := e.queue.Depths(ctx)
			if err == nil {
				e.metrics.SetQueueDepths(depths)
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

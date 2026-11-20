package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/handler"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/config"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/provider"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/queue"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/realtime"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/repository/postgres"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal application error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Distributed Tracing
	shutdownTracer, err := observability.InitTracing(ctx, cfg.Tracing)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracer(shutCtx)
	}()

	metrics := observability.NewMetrics()

	// PostgreSQL Connection Pool & Versioned Migrations
	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	if cfg.Database.AutoMigrate {
		logger.Info("running database migrations")
		if err := postgres.RunMigrations(ctx, pool); err != nil {
			return fmt.Errorf("run migrations: %w", err)
		}
	}

	// Redis Client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer func() { _ = redisClient.Close() }()

	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		return fmt.Errorf("connect redis: %w", err)
	}
	cancelPing()

	// Repositories
	notifRepo := postgres.NewNotificationRepo(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	attemptRepo := postgres.NewAttemptRepo(pool)
	templateRepo := postgres.NewTemplateRepo(pool)

	// Queue, Rate Limiter, and Real-time Pub/Sub
	redisQueue := queue.NewRedisQueue(redisClient)
	rateLimiter := queue.NewRedisRateLimiter(redisClient, map[domain.Channel]int{
		domain.ChannelSMS:   cfg.RateLimits.SMS,
		domain.ChannelEmail: cfg.RateLimits.Email,
		domain.ChannelPush:  cfg.RateLimits.Push,
	})
	eventPub := realtime.NewEventPublisher(redisClient)
	wsHub := realtime.NewHub(redisClient, logger)

	// External Provider wrapped in Per-Channel Circuit Breaker
	webhookProv := provider.NewWebhookProvider(cfg.Webhook.URL, cfg.Webhook.Timeout)
	protectedProv := provider.NewCircuitBreakerProvider(
		webhookProv,
		cfg.CircuitBreaker.FailureThreshold,
		cfg.CircuitBreaker.Cooldown,
		cfg.CircuitBreaker.HalfOpenMaxProbes,
		func(ch string, st provider.CircuitState) {
			metrics.SetCircuitBreakerState(ch, string(st))
			logger.Warn("circuit breaker state changed",
				slog.String("channel", ch),
				slog.String("state", string(st)),
			)
		},
	)

	// Application Services
	notifSvc := service.NewNotificationService(notifRepo, templateRepo, eventPub, metrics, logger, cfg.Worker.MaxRetryAttempts)
	templateSvc := service.NewTemplateService(templateRepo)

	// Start Worker Engine (when mode is "all" or "worker")
	var engine *worker.Engine
	if cfg.AppMode == "all" || cfg.AppMode == "worker" {
		backoff := worker.NewBackoffPolicy(cfg.Worker.RetryBaseBackoff, cfg.Worker.RetryMaxBackoff)
		processor := worker.NewProcessor(
			notifRepo,
			attemptRepo,
			redisQueue,
			rateLimiter,
			protectedProv,
			eventPub,
			metrics,
			backoff,
			logger,
		)
		outboxRelay := worker.NewOutboxRelay(
			outboxRepo,
			redisQueue,
			eventPub,
			logger,
			cfg.Worker.OutboxPollInterval,
			cfg.Worker.OutboxBatchSize,
		)
		scheduler := worker.NewScheduler(
			notifRepo,
			eventPub,
			logger,
			cfg.Worker.SchedulerPollInterval,
			cfg.Worker.ProcessingLeaseDuration,
		)
		engine = worker.NewEngine(cfg.Worker, redisQueue, processor, outboxRelay, scheduler, metrics, logger)
		engine.Start(ctx)
	}

	// Start HTTP Server & WebSocket Hub (when mode is "all" or "api")
	var httpSrv *http.Server
	serverErrCh := make(chan error, 1)
	if cfg.AppMode == "all" || cfg.AppMode == "api" {
		go func() {
			if err := wsHub.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("websocket hub stopped unexpectedly", slog.Any("error", err))
			}
		}()

		router := api.NewRouter(api.RouterDeps{
			NotificationSvc: notifSvc,
			TemplateSvc:     templateSvc,
			Metrics:         metrics,
			QueueDepths:     redisQueue,
			HealthChecks: map[string]handler.DependencyCheck{
				"postgres": func(c context.Context) error { return pool.Ping(c) },
				"redis":    func(c context.Context) error { return redisClient.Ping(c).Err() },
			},
			Hub:    wsHub,
			Logger: logger,
		})

		httpSrv = &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
			Handler:      router,
			ReadTimeout:  cfg.HTTP.ReadTimeout,
			WriteTimeout: cfg.HTTP.WriteTimeout,
			IdleTimeout:  cfg.HTTP.IdleTimeout,
		}

		go func() {
			logger.Info("HTTP server listening",
				slog.Int("port", cfg.HTTP.Port),
				slog.String("mode", cfg.AppMode),
				slog.String("webhook_url", cfg.Webhook.URL),
			)
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serverErrCh <- err
			}
		}()
	}

	// Wait for termination signal or fatal HTTP server error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; draining gracefully")
	case err := <-serverErrCh:
		stop()
		return fmt.Errorf("http server error: %w", err)
	}

	if httpSrv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutCtx); err != nil {
			logger.Error("http server graceful shutdown error", slog.Any("error", err))
		}
	}

	if engine != nil {
		engine.Wait()
	}

	logger.Info("application exited cleanly")
	return nil
}

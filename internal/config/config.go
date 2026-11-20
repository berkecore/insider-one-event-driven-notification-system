package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	AppMode  string
	AppEnv   string
	LogLevel string

	HTTP           HTTPConfig
	Database       DatabaseConfig
	Redis          RedisConfig
	Webhook        WebhookConfig
	RateLimits     RateLimitConfig
	Worker         WorkerConfig
	CircuitBreaker CircuitBreakerConfig
	Tracing        TracingConfig
}

type HTTPConfig struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type DatabaseConfig struct {
	URL         string
	MaxConns    int32
	MinConns    int32
	AutoMigrate bool
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type WebhookConfig struct {
	URL     string
	Timeout time.Duration
}

type RateLimitConfig struct {
	SMS   int
	Email int
	Push  int
}

type WorkerConfig struct {
	WorkersPerChannel       int
	MaxRetryAttempts        int
	RetryBaseBackoff        time.Duration
	RetryMaxBackoff         time.Duration
	OutboxPollInterval      time.Duration
	OutboxBatchSize         int
	SchedulerPollInterval   time.Duration
	RetryPromoterInterval   time.Duration
	ProcessingLeaseDuration time.Duration
}

type CircuitBreakerConfig struct {
	FailureThreshold  int
	Cooldown          time.Duration
	HalfOpenMaxProbes int
}

type TracingConfig struct {
	Exporter     string // noop | stdout | otlp
	ServiceName  string
	OTLPEndpoint string
}

// Load parses configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		AppMode:  strings.ToLower(getEnv("APP_MODE", "all")),
		AppEnv:   getEnv("APP_ENV", "development"),
		LogLevel: strings.ToLower(getEnv("LOG_LEVEL", "info")),
		HTTP: HTTPConfig{
			Port:         getInt("HTTP_PORT", 8080),
			ReadTimeout:  getDuration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: getDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
			IdleTimeout:  getDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		},
		Database: DatabaseConfig{
			URL:         getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/notifications?sslmode=disable"),
			MaxConns:    int32(getInt("DB_MAX_CONNS", 25)),
			MinConns:    int32(getInt("DB_MIN_CONNS", 5)),
			AutoMigrate: getBool("DB_AUTO_MIGRATE", true),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getInt("REDIS_DB", 0),
		},
		Webhook: WebhookConfig{
			URL:     getEnv("WEBHOOK_URL", "http://localhost:8081/webhook"),
			Timeout: getDuration("WEBHOOK_TIMEOUT", 10*time.Second),
		},
		RateLimits: RateLimitConfig{
			SMS:   getInt("RATE_LIMIT_SMS", 100),
			Email: getInt("RATE_LIMIT_EMAIL", 100),
			Push:  getInt("RATE_LIMIT_PUSH", 100),
		},
		Worker: WorkerConfig{
			WorkersPerChannel:       getInt("WORKERS_PER_CHANNEL", 8),
			MaxRetryAttempts:        getInt("MAX_RETRY_ATTEMPTS", 5),
			RetryBaseBackoff:        getDuration("RETRY_BASE_BACKOFF", 1*time.Second),
			RetryMaxBackoff:         getDuration("RETRY_MAX_BACKOFF", 60*time.Second),
			OutboxPollInterval:      getDuration("OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
			OutboxBatchSize:         getInt("OUTBOX_BATCH_SIZE", 100),
			SchedulerPollInterval:   getDuration("SCHEDULER_POLL_INTERVAL", 1*time.Second),
			RetryPromoterInterval:   getDuration("RETRY_PROMOTER_INTERVAL", 250*time.Millisecond),
			ProcessingLeaseDuration: getDuration("PROCESSING_LEASE_DURATION", 60*time.Second),
		},
		CircuitBreaker: CircuitBreakerConfig{
			FailureThreshold:  getInt("CB_FAILURE_THRESHOLD", 5),
			Cooldown:          getDuration("CB_COOLDOWN", 15*time.Second),
			HalfOpenMaxProbes: getInt("CB_HALF_OPEN_MAX_PROBES", 2),
		},
		Tracing: TracingConfig{
			Exporter:     strings.ToLower(getEnv("OTEL_EXPORTER", "noop")),
			ServiceName:  getEnv("OTEL_SERVICE_NAME", "notification-service"),
			OTLPEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks that configuration values are within valid operational bounds.
func (c *Config) Validate() error {
	switch c.AppMode {
	case "all", "api", "worker":
	default:
		return fmt.Errorf("invalid APP_MODE %q: must be one of all, api, worker", c.AppMode)
	}
	if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
		return fmt.Errorf("invalid HTTP_PORT %d", c.HTTP.Port)
	}
	if c.Database.URL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("REDIS_ADDR is required")
	}
	if c.Webhook.URL == "" {
		return fmt.Errorf("WEBHOOK_URL is required")
	}
	if c.RateLimits.SMS <= 0 || c.RateLimits.Email <= 0 || c.RateLimits.Push <= 0 {
		return fmt.Errorf("channel rate limits must be positive")
	}
	if c.Worker.WorkersPerChannel <= 0 {
		return fmt.Errorf("WORKERS_PER_CHANNEL must be positive")
	}
	if c.Worker.MaxRetryAttempts <= 0 {
		return fmt.Errorf("MAX_RETRY_ATTEMPTS must be positive")
	}
	switch c.Tracing.Exporter {
	case "noop", "stdout", "otlp":
	default:
		return fmt.Errorf("invalid OTEL_EXPORTER %q: must be one of noop, stdout, otlp", c.Tracing.Exporter)
	}
	return nil
}

// RateLimitForChannel returns the configured max messages per second for a channel.
func (c *Config) RateLimitForChannel(channel string) int {
	switch strings.ToLower(channel) {
	case "sms":
		return c.RateLimits.SMS
	case "email":
		return c.RateLimits.Email
	case "push":
		return c.RateLimits.Push
	default:
		return 100
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func getInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return parsed
}

func getBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return parsed
}

func getDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return parsed
}

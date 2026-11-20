package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected default config to load cleanly, got error: %v", err)
	}
	if cfg.HTTP.Port != 8080 {
		t.Errorf("expected default HTTP port 8080, got %d", cfg.HTTP.Port)
	}
	if cfg.RateLimits.SMS != 100 || cfg.RateLimits.Email != 100 || cfg.RateLimits.Push != 100 {
		t.Errorf("expected default channel rate limits of 100, got %+v", cfg.RateLimits)
	}
	if cfg.Worker.MaxRetryAttempts != 5 {
		t.Errorf("expected default max retries 5, got %d", cfg.Worker.MaxRetryAttempts)
	}
}

func TestLoadOverridesAndValidation(t *testing.T) {
	t.Setenv("APP_MODE", "worker")
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("RATE_LIMIT_SMS", "50")
	t.Setenv("RETRY_BASE_BACKOFF", "2s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AppMode != "worker" || cfg.HTTP.Port != 9090 || cfg.RateLimitForChannel("sms") != 50 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
	if cfg.Worker.RetryBaseBackoff != 2*time.Second {
		t.Errorf("expected 2s retry base backoff, got %v", cfg.Worker.RetryBaseBackoff)
	}

	t.Setenv("APP_MODE", "invalid")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for invalid APP_MODE")
	}
}

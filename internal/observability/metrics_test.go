package observability

import (
	"context"
	"testing"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/config"
)

func TestMetricsSnapshotAndRates(t *testing.T) {
	m := NewMetrics()

	m.IncCreated("sms", "high")
	m.IncCreated("sms", "normal")
	m.IncCreated("email", "low")

	m.RecordDelivered("sms", 20*time.Millisecond)
	m.RecordDelivered("sms", 40*time.Millisecond)
	m.RecordDelivered("sms", 100*time.Millisecond)
	m.RecordFailed("sms", 15*time.Millisecond)
	m.RecordRetried("sms", 10*time.Millisecond)
	m.SetCircuitBreakerState("sms", "open")

	snap := m.Snapshot(map[string]QueueDepthSnapshot{
		"sms": {High: 2, Normal: 3, Low: 1, Retry: 1, DLQ: 0, Total: 7},
	})

	if snap.Delivery.TotalDelivered != 3 {
		t.Fatalf("expected 3 delivered, got %d", snap.Delivery.TotalDelivered)
	}
	if snap.Delivery.TotalFailed != 1 {
		t.Fatalf("expected 1 failed, got %d", snap.Delivery.TotalFailed)
	}
	if snap.Delivery.SuccessRatePercent != 75 {
		t.Fatalf("expected 75%% success rate, got %.2f", snap.Delivery.SuccessRatePercent)
	}
	if snap.Delivery.FailureRatePercent != 25 {
		t.Fatalf("expected 25%% failure rate, got %.2f", snap.Delivery.FailureRatePercent)
	}
	if snap.CircuitBreakers["sms"] != "open" {
		t.Fatalf("expected sms circuit breaker open, got %s", snap.CircuitBreakers["sms"])
	}
	if snap.Queues["sms"].Total != 7 {
		t.Fatalf("expected sms queue total 7, got %d", snap.Queues["sms"].Total)
	}
	if snap.Delivery.P95LatencyMs < 40 {
		t.Fatalf("expected p95 latency >= 40ms, got %.2f", snap.Delivery.P95LatencyMs)
	}
}

func TestCorrelationIDAndNoopTracing(t *testing.T) {
	shutdown, err := InitTracing(context.Background(), config.TracingConfig{
		Exporter:    "noop",
		ServiceName: "test-service",
	})
	if err != nil {
		t.Fatalf("InitTracing noop failed: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	ctx := WithCorrelationID(context.Background(), "corr-123")
	if got := CorrelationIDFromContext(ctx); got != "corr-123" {
		t.Fatalf("expected corr-123, got %q", got)
	}
	if LoggerWithContext(ctx, NewLogger("debug")) == nil {
		t.Fatal("expected non-nil logger")
	}
}

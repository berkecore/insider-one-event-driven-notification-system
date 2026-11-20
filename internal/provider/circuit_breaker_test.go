package provider

import (
	"context"
	"testing"
	"time"
)

type stubProvider struct {
	fn func(ctx context.Context, req DeliveryRequest) (*DeliveryResponse, error)
}

func (s *stubProvider) Deliver(ctx context.Context, req DeliveryRequest) (*DeliveryResponse, error) {
	return s.fn(ctx, req)
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	var transitions []CircuitState

	cb := NewCircuitBreaker("sms", 3, 10*time.Second, 2, func(_ string, st CircuitState) {
		transitions = append(transitions, st)
	})
	cb.clock = func() time.Time { return now }

	// 3 consecutive failures open the breaker
	for i := 0; i < 3; i++ {
		if ok, _ := cb.Allow(); !ok {
			t.Fatalf("attempt %d should be allowed", i)
		}
		cb.RecordFailure()
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected open state, got %s", cb.State())
	}

	// Requests during cooldown are blocked
	if ok, retryAfter := cb.Allow(); ok || retryAfter != 10*time.Second {
		t.Fatalf("expected blocked with 10s retryAfter, got ok=%v retryAfter=%v", ok, retryAfter)
	}

	// Advance past cooldown -> half-open
	now = now.Add(11 * time.Second)
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected half-open after cooldown, got %s", cb.State())
	}

	// Two successful probes close the circuit
	for i := 0; i < 2; i++ {
		if ok, _ := cb.Allow(); !ok {
			t.Fatalf("probe %d should be allowed", i)
		}
		cb.RecordSuccess()
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected closed after successful probes, got %s", cb.State())
	}
	if len(transitions) != 3 || transitions[0] != StateOpen || transitions[1] != StateHalfOpen || transitions[2] != StateClosed {
		t.Fatalf("unexpected transition sequence: %v", transitions)
	}
}

func TestCircuitBreakerProviderIsolatesChannelsAndPermanentErrors(t *testing.T) {
	stub := &stubProvider{
		fn: func(_ context.Context, req DeliveryRequest) (*DeliveryResponse, error) {
			if req.Channel == "sms" {
				return nil, &DeliveryError{StatusCode: 503, Message: "down", Retryable: true}
			}
			if req.Channel == "email" {
				return nil, &DeliveryError{StatusCode: 400, Message: "bad request", Retryable: false}
			}
			return &DeliveryResponse{MessageID: "ok", Status: "accepted"}, nil
		},
	}

	cbp := NewCircuitBreakerProvider(stub, 2, 10*time.Second, 1, nil)

	// Two 503s trip SMS circuit
	_, _ = cbp.Deliver(context.Background(), DeliveryRequest{Channel: "sms"})
	_, _ = cbp.Deliver(context.Background(), DeliveryRequest{Channel: "sms"})
	_, err := cbp.Deliver(context.Background(), DeliveryRequest{Channel: "sms"})
	if _, ok := IsCircuitOpen(err); !ok {
		t.Fatalf("expected CircuitOpenError for sms, got %v", err)
	}

	// Permanent 400 errors on Email must NOT trip Email circuit
	for i := 0; i < 4; i++ {
		_, err := cbp.Deliver(context.Background(), DeliveryRequest{Channel: "email"})
		if _, ok := IsCircuitOpen(err); ok {
			t.Fatalf("400 errors should not open email circuit breaker")
		}
	}

	// Push channel remains healthy
	resp, err := cbp.Deliver(context.Background(), DeliveryRequest{Channel: "push"})
	if err != nil || resp.MessageID != "ok" {
		t.Fatalf("expected push to succeed, got resp=%+v err=%v", resp, err)
	}
}

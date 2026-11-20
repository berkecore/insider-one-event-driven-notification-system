package worker

import (
	"testing"
	"time"
)

func TestBackoffPolicyBoundsAndRetryAfter(t *testing.T) {
	p := NewBackoffPolicy(1*time.Second, 16*time.Second)
	p.randFloat = func() float64 { return 0.5 } // deterministic midpoint of equal jitter

	// attempt 1: cap=1s -> 0.5s + 0.5*0.5s = 750ms
	if got := p.NextDelay(1, 0); got != 750*time.Millisecond {
		t.Fatalf("attempt 1: expected 750ms, got %v", got)
	}
	// attempt 2: cap=2s -> 1.5s
	if got := p.NextDelay(2, 0); got != 1500*time.Millisecond {
		t.Fatalf("attempt 2: expected 1500ms, got %v", got)
	}
	// attempt 10: capped at Max=16s -> 12s
	if got := p.NextDelay(10, 0); got != 12*time.Second {
		t.Fatalf("attempt 10: expected 12s, got %v", got)
	}
	// Provider Retry-After larger than backoff is honored (capped at Max)
	if got := p.NextDelay(1, 5*time.Second); got != 5*time.Second {
		t.Fatalf("retry-after 5s: expected 5s, got %v", got)
	}
	if got := p.NextDelay(1, 90*time.Second); got != 16*time.Second {
		t.Fatalf("retry-after capped at max: expected 16s, got %v", got)
	}
}

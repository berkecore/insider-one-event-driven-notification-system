package provider

import (
	"context"
	"errors"
	"sync"
	"time"
)

// CircuitState represents the current state of a circuit breaker.
type CircuitState string

const (
	StateClosed   CircuitState = "closed"
	StateOpen     CircuitState = "open"
	StateHalfOpen CircuitState = "half-open"
)

// StateChangeCallback is invoked when a channel circuit breaker transitions states.
type StateChangeCallback func(channel string, state CircuitState)

// CircuitBreaker protects downstream notification channels from cascading failures.
type CircuitBreaker struct {
	mu                sync.Mutex
	channel           string
	failureThreshold  int
	cooldown          time.Duration
	halfOpenMaxProbes int
	clock             func() time.Time
	onStateChange     StateChangeCallback

	state               CircuitState
	consecutiveFailures int
	openedAt            time.Time
	halfOpenInFlight    int
	halfOpenSuccesses   int
}

// NewCircuitBreaker constructs a channel circuit breaker starting in the Closed state.
func NewCircuitBreaker(channel string, failureThreshold int, cooldown time.Duration, halfOpenMaxProbes int, onChange StateChangeCallback) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 5
	}
	if cooldown <= 0 {
		cooldown = 15 * time.Second
	}
	if halfOpenMaxProbes <= 0 {
		halfOpenMaxProbes = 2
	}
	return &CircuitBreaker{
		channel:           channel,
		failureThreshold:  failureThreshold,
		cooldown:          cooldown,
		halfOpenMaxProbes: halfOpenMaxProbes,
		clock:             time.Now,
		onStateChange:     onChange,
		state:             StateClosed,
	}
}

// State returns the effective current state (accounting for elapsed cooldown).
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == StateOpen && cb.clock().Sub(cb.openedAt) >= cb.cooldown {
		return StateHalfOpen
	}
	return cb.state
}

// Allow checks whether a request is permitted to execute.
// If rejected, it returns the remaining cooldown duration.
func (cb *CircuitBreaker) Allow() (bool, time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := cb.clock()
	switch cb.state {
	case StateClosed:
		return true, 0
	case StateOpen:
		elapsed := now.Sub(cb.openedAt)
		if elapsed < cb.cooldown {
			return false, cb.cooldown - elapsed
		}
		cb.transitionLocked(StateHalfOpen)
		cb.halfOpenInFlight = 1
		cb.halfOpenSuccesses = 0
		return true, 0
	case StateHalfOpen:
		if cb.halfOpenInFlight >= cb.halfOpenMaxProbes {
			return false, time.Second
		}
		cb.halfOpenInFlight++
		return true, 0
	default:
		return true, 0
	}
}

// RecordSuccess notifies the breaker that a downstream request succeeded.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures = 0
	if cb.state == StateHalfOpen {
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.halfOpenMaxProbes {
			cb.transitionLocked(StateClosed)
		}
	}
}

// RecordFailure notifies the breaker of a transient/downstream provider failure.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.consecutiveFailures++
		if cb.consecutiveFailures >= cb.failureThreshold {
			cb.openedAt = cb.clock()
			cb.transitionLocked(StateOpen)
		}
	case StateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.openedAt = cb.clock()
		cb.transitionLocked(StateOpen)
	}
}

func (cb *CircuitBreaker) transitionLocked(next CircuitState) {
	if cb.state == next {
		return
	}
	cb.state = next
	if next == StateClosed {
		cb.consecutiveFailures = 0
		cb.halfOpenInFlight = 0
		cb.halfOpenSuccesses = 0
	}
	if cb.onStateChange != nil {
		cb.onStateChange(cb.channel, next)
	}
}

// CircuitBreakerProvider wraps a Provider with independent per-channel circuit breakers.
type CircuitBreakerProvider struct {
	inner    Provider
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
	cfg      struct {
		threshold int
		cooldown  time.Duration
		maxProbes int
		onChange  StateChangeCallback
	}
}

// NewCircuitBreakerProvider wraps inner with per-channel circuit breaker protection.
func NewCircuitBreakerProvider(inner Provider, failureThreshold int, cooldown time.Duration, halfOpenMaxProbes int, onChange StateChangeCallback) *CircuitBreakerProvider {
	p := &CircuitBreakerProvider{
		inner:    inner,
		breakers: make(map[string]*CircuitBreaker),
	}
	p.cfg.threshold = failureThreshold
	p.cfg.cooldown = cooldown
	p.cfg.maxProbes = halfOpenMaxProbes
	p.cfg.onChange = onChange

	for _, ch := range []string{"sms", "email", "push"} {
		p.breakers[ch] = NewCircuitBreaker(ch, failureThreshold, cooldown, halfOpenMaxProbes, onChange)
	}
	return p
}

func (p *CircuitBreakerProvider) breakerFor(channel string) *CircuitBreaker {
	p.mu.RLock()
	cb, ok := p.breakers[channel]
	p.mu.RUnlock()
	if ok {
		return cb
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if cb, ok = p.breakers[channel]; !ok {
		cb = NewCircuitBreaker(channel, p.cfg.threshold, p.cfg.cooldown, p.cfg.maxProbes, p.cfg.onChange)
		p.breakers[channel] = cb
	}
	return cb
}

// Deliver enforces the channel circuit breaker before invoking the underlying provider.
// Only retryable/downstream errors trip the circuit breaker; permanent 4xx client errors do not.
func (p *CircuitBreakerProvider) Deliver(ctx context.Context, req DeliveryRequest) (*DeliveryResponse, error) {
	cb := p.breakerFor(req.Channel)
	allowed, retryAfter := cb.Allow()
	if !allowed {
		return nil, &CircuitOpenError{
			Channel:    req.Channel,
			RetryAfter: retryAfter,
		}
	}

	resp, err := p.inner.Deliver(ctx, req)
	if err != nil {
		var derr *DeliveryError
		if !errors.As(err, &derr) || derr.Retryable {
			cb.RecordFailure()
		} else {
			// A 4xx client validation error means the provider itself is healthy.
			cb.RecordSuccess()
		}
		return nil, err
	}

	cb.RecordSuccess()
	return resp, nil
}

// States returns a snapshot of every channel's current circuit breaker state.
func (p *CircuitBreakerProvider) States() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]string, len(p.breakers))
	for ch, cb := range p.breakers {
		out[ch] = string(cb.State())
	}
	return out
}

package worker

import (
	"math"
	"math/rand/v2"
	"time"
)

// BackoffPolicy computes exponential backoff delays with equal jitter.
type BackoffPolicy struct {
	Base      time.Duration
	Max       time.Duration
	randFloat func() float64
}

// NewBackoffPolicy creates a BackoffPolicy with safe defaults.
func NewBackoffPolicy(base, max time.Duration) BackoffPolicy {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = 60 * time.Second
	}
	return BackoffPolicy{
		Base:      base,
		Max:       max,
		randFloat: rand.Float64,
	}
}

// NextDelay returns the delay before the next retry using equal jitter:
// cap = min(Max, Base * 2^(attempt-1))
// delay = cap/2 + rand(0, cap/2)
// If providerRetryAfter exceeds delay, it is honored up to Max.
func (p BackoffPolicy) NextDelay(attempt int, providerRetryAfter time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := math.Pow(2, float64(attempt-1))
	capped := time.Duration(float64(p.Base) * mult)
	if capped <= 0 || capped > p.Max {
		capped = p.Max
	}

	rf := p.randFloat
	if rf == nil {
		rf = rand.Float64
	}
	half := capped / 2
	delay := half + time.Duration(rf()*float64(capped-half))

	if providerRetryAfter > delay {
		if providerRetryAfter > p.Max {
			return p.Max
		}
		return providerRetryAfter
	}
	return delay
}

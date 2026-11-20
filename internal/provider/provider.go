package provider

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DeliveryRequest represents the outbound payload sent to the external notification provider.
type DeliveryRequest struct {
	To             string `json:"to"`
	Channel        string `json:"channel"`
	Content        string `json:"content"`
	NotificationID string `json:"-"`
	CorrelationID  string `json:"-"`
	AttemptNumber  int    `json:"-"`
}

// DeliveryResponse matches the expected external webhook.site 202 response format.
type DeliveryResponse struct {
	MessageID  string `json:"messageId"`
	Status     string `json:"status"`
	Timestamp  string `json:"timestamp"`
	StatusCode int    `json:"-"`
}

// DeliveryError captures provider failure details and whether the failure should be retried.
type DeliveryError struct {
	StatusCode int
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	Err        error
}

func (e *DeliveryError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("provider HTTP %d: %s", e.StatusCode, e.Message)
	}
	if e.Err != nil {
		return fmt.Sprintf("provider error: %s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("provider error: %s", e.Message)
}

func (e *DeliveryError) Unwrap() error {
	return e.Err
}

// CircuitOpenError indicates the channel's circuit breaker is open and delivery was short-circuited.
type CircuitOpenError struct {
	Channel    string
	RetryAfter time.Duration
}

func (e *CircuitOpenError) Error() string {
	return fmt.Sprintf("circuit breaker open for channel %s (retry after %s)", e.Channel, e.RetryAfter)
}

// IsCircuitOpen returns the CircuitOpenError if err represents an open circuit breaker.
func IsCircuitOpen(err error) (*CircuitOpenError, bool) {
	var ce *CircuitOpenError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}

// Provider delivers a notification to an external provider (e.g. webhook.site).
type Provider interface {
	Deliver(ctx context.Context, req DeliveryRequest) (*DeliveryResponse, error)
}

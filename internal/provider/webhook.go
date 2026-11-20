package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const maxErrorBodyBytes = 2048

// WebhookProvider delivers notifications via HTTP POST to a webhook.site-compatible endpoint.
type WebhookProvider struct {
	url    string
	client *http.Client
}

// NewWebhookProvider constructs a tuned HTTP client for external webhook deliveries.
func NewWebhookProvider(webhookURL string, timeout time.Duration) *WebhookProvider {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	transport := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}
	return &WebhookProvider{
		url: webhookURL,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

// Deliver sends POST { "to", "channel", "content" } and expects HTTP 202 (accepting 2xx)
// with { "messageId", "status", "timestamp" }.
func (p *WebhookProvider) Deliver(ctx context.Context, req DeliveryRequest) (*DeliveryResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, &DeliveryError{
			Message:   "failed to marshal delivery request",
			Retryable: false,
			Err:       err,
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(payload))
	if err != nil {
		return nil, &DeliveryError{
			Message:   "failed to construct webhook request",
			Retryable: false,
			Err:       err,
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if req.CorrelationID != "" {
		httpReq.Header.Set("X-Correlation-ID", req.CorrelationID)
	}
	if req.NotificationID != "" {
		httpReq.Header.Set("X-Notification-ID", req.NotificationID)
	}
	if req.AttemptNumber > 0 {
		httpReq.Header.Set("X-Attempt-Number", strconv.Itoa(req.AttemptNumber))
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, &DeliveryError{
			Message:   "network or timeout failure calling webhook provider",
			Retryable: true,
			Err:       err,
		}
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return nil, &DeliveryError{
			StatusCode: resp.StatusCode,
			Message:    "failed to read provider response body",
			Retryable:  true,
			Err:        readErr,
		}
	}

	if resp.StatusCode == http.StatusAccepted || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		var parsed DeliveryResponse
		if len(bytes.TrimSpace(body)) > 0 {
			_ = json.Unmarshal(body, &parsed)
		}
		if parsed.MessageID == "" {
			parsed.MessageID = uuid.NewString()
		}
		if parsed.Status == "" {
			parsed.Status = "accepted"
		}
		if parsed.Timestamp == "" {
			parsed.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}
		parsed.StatusCode = resp.StatusCode
		return &parsed, nil
	}

	retryable := isRetryableStatus(resp.StatusCode)
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
	snippet := strings.TrimSpace(string(body))
	if len(snippet) > maxErrorBodyBytes {
		snippet = snippet[:maxErrorBodyBytes]
	}
	if snippet == "" {
		snippet = http.StatusText(resp.StatusCode)
	}

	return nil, &DeliveryError{
		StatusCode: resp.StatusCode,
		Message:    fmt.Sprintf("%s", snippet),
		Retryable:  retryable,
		RetryAfter: retryAfter,
	}
}

func isRetryableStatus(code int) bool {
	if code >= 500 && code <= 599 {
		return true
	}
	return code == http.StatusTooManyRequests || code == http.StatusRequestTimeout
}

func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if secs, err := strconv.Atoi(header); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(header); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

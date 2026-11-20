package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebhookProviderAccepted202(t *testing.T) {
	var received DeliveryRequest
	var gotCorrID string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCorrID = r.Header.Get("X-Correlation-ID")
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"msg-abc-123","status":"accepted","timestamp":"2026-11-20T12:00:00Z"}`))
	}))
	defer srv.Close()

	wp := NewWebhookProvider(srv.URL, 2*time.Second)
	resp, err := wp.Deliver(context.Background(), DeliveryRequest{
		To:             "+905551234567",
		Channel:        "sms",
		Content:        "Hello Insider",
		NotificationID: "notif-1",
		CorrelationID:  "corr-xyz",
		AttemptNumber:  1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.MessageID != "msg-abc-123" || resp.Status != "accepted" || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if received.To != "+905551234567" || received.Channel != "sms" || received.Content != "Hello Insider" {
		t.Fatalf("unexpected request payload: %+v", received)
	}
	if gotCorrID != "corr-xyz" {
		t.Fatalf("expected correlation ID header corr-xyz, got %q", gotCorrID)
	}
}

func TestWebhookProviderErrorClassification(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		retryAfterHdr  string
		wantRetryable  bool
		wantRetryAfter time.Duration
	}{
		{"server 503 is retryable", http.StatusServiceUnavailable, "", true, 0},
		{"rate limit 429 honors Retry-After", http.StatusTooManyRequests, "3", true, 3 * time.Second},
		{"client 400 is permanent", http.StatusBadRequest, "", false, 0},
		{"client 422 is permanent", http.StatusUnprocessableEntity, "", false, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.retryAfterHdr != "" {
					w.Header().Set("Retry-After", tc.retryAfterHdr)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":"boom"}`))
			}))
			defer srv.Close()

			wp := NewWebhookProvider(srv.URL, 2*time.Second)
			_, err := wp.Deliver(context.Background(), DeliveryRequest{To: "+905551234567", Channel: "sms", Content: "hi"})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var derr *DeliveryError
			if !errors.As(err, &derr) {
				t.Fatalf("expected DeliveryError, got %T", err)
			}
			if derr.Retryable != tc.wantRetryable {
				t.Errorf("expected retryable=%v, got %v", tc.wantRetryable, derr.Retryable)
			}
			if derr.StatusCode != tc.status {
				t.Errorf("expected status %d, got %d", tc.status, derr.StatusCode)
			}
			if derr.RetryAfter != tc.wantRetryAfter {
				t.Errorf("expected RetryAfter=%v, got %v", tc.wantRetryAfter, derr.RetryAfter)
			}
		})
	}
}

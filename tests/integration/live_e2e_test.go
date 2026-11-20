//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/google/uuid"
)

func baseURL() string {
	if v := os.Getenv("API_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func TestLiveDockerStackEndToEnd(t *testing.T) {
	url := baseURL()
	client := &http.Client{Timeout: 10 * time.Second}

	// 1. Readiness check (PostgreSQL + Redis)
	resp, err := client.Get(url + "/ready")
	if err != nil {
		t.Fatalf("GET /ready failed (is docker compose up?): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on /ready, got %d", resp.StatusCode)
	}

	// 2. Create single notification with Idempotency-Key and verify delivery via mock-provider
	idemKey := "live-idem-" + uuid.NewString()
	reqBody := `{"recipient":"+905551234567","channel":"sms","content":"Live integration test SMS","priority":"high"}`
	req, _ := http.NewRequest(http.MethodPost, url+"/api/v1/notifications", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idemKey)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("POST /api/v1/notifications failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	var created domain.Notification
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()

	// Replay same idempotency key -> 200 OK with same ID
	reqReplay, _ := http.NewRequest(http.MethodPost, url+"/api/v1/notifications", bytes.NewBufferString(reqBody))
	reqReplay.Header.Set("Content-Type", "application/json")
	reqReplay.Header.Set("Idempotency-Key", idemKey)
	respReplay, err := client.Do(reqReplay)
	if err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
	var replayed domain.Notification
	_ = json.NewDecoder(respReplay.Body).Decode(&replayed)
	respReplay.Body.Close()
	if respReplay.StatusCode != http.StatusOK || replayed.ID != created.ID {
		t.Fatalf("expected 200 replay with id %s, got status=%d id=%s", created.ID, respReplay.StatusCode, replayed.ID)
	}

	// Wait for worker delivery
	deadline := time.Now().Add(10 * time.Second)
	var delivered domain.Notification
	for time.Now().Before(deadline) {
		r, err := client.Get(fmt.Sprintf("%s/api/v1/notifications/%s", url, created.ID))
		if err == nil {
			_ = json.NewDecoder(r.Body).Decode(&delivered)
			r.Body.Close()
			if delivered.Status == domain.StatusDelivered {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if delivered.Status != domain.StatusDelivered || delivered.ProviderMessageID == nil {
		t.Fatalf("expected notification delivered with provider_message_id, got %+v", delivered)
	}

	// 3. Batch creation (20 items across channels & priorities)
	items := make([]map[string]any, 20)
	for i := range items {
		items[i] = map[string]any{
			"recipient": fmt.Sprintf("user%d@example.com", i),
			"channel":   "email",
			"content":   fmt.Sprintf("Batch message #%d", i),
			"priority":  "normal",
		}
	}
	batchPayload, _ := json.Marshal(map[string]any{"notifications": items})
	bResp, err := client.Post(url+"/api/v1/notifications/batch", "application/json", bytes.NewReader(batchPayload))
	if err != nil || bResp.StatusCode != http.StatusCreated {
		t.Fatalf("batch create failed: err=%v resp=%v", err, bResp)
	}
	var batchSum domain.BatchSummary
	_ = json.NewDecoder(bResp.Body).Decode(&batchSum)
	bResp.Body.Close()

	// 4. Scheduled notification -> Cancel before delivery
	future := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	schedPayload := fmt.Sprintf(`{"recipient":"device_token_12345678","channel":"push","content":"Scheduled push","scheduled_at":"%s"}`, future)
	sResp, err := client.Post(url+"/api/v1/notifications", "application/json", bytes.NewBufferString(schedPayload))
	if err != nil || sResp.StatusCode != http.StatusCreated {
		t.Fatalf("scheduled create failed: err=%v", err)
	}
	var sched domain.Notification
	_ = json.NewDecoder(sResp.Body).Decode(&sched)
	sResp.Body.Close()
	if sched.Status != domain.StatusScheduled {
		t.Fatalf("expected scheduled status, got %s", sched.Status)
	}

	cResp, err := client.Post(fmt.Sprintf("%s/api/v1/notifications/%s/cancel", url, sched.ID), "application/json", nil)
	if err != nil || cResp.StatusCode != http.StatusOK {
		t.Fatalf("cancel failed: err=%v", err)
	}
	var cancelled domain.Notification
	_ = json.NewDecoder(cResp.Body).Decode(&cancelled)
	cResp.Body.Close()
	if cancelled.Status != domain.StatusCancelled {
		t.Fatalf("expected cancelled status, got %s", cancelled.Status)
	}

	// 5. Real-time metrics endpoint
	mResp, err := client.Get(url + "/api/v1/metrics")
	if err != nil || mResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/metrics failed: %v", err)
	}
	var snap observability.MetricsSnapshot
	_ = json.NewDecoder(mResp.Body).Decode(&snap)
	mResp.Body.Close()
	if snap.Delivery.TotalDelivered < 1 {
		t.Fatalf("expected >= 1 delivered notification in live metrics, got %+v", snap.Delivery)
	}
}

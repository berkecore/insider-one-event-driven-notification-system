package main

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type webhookRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type webhookResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	port := envOr("MOCK_PROVIDER_PORT", "8081")
	latencyMs := envInt("MOCK_LATENCY_MS", 15)
	failRate := envFloat("MOCK_FAIL_RATE", 0.0)
	failStatus := envInt("MOCK_FAIL_STATUS", http.StatusServiceUnavailable)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"mock-webhook-provider"}`))
	})

	handleWebhook := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		delay := latencyMs
		if q := r.URL.Query().Get("delay_ms"); q != "" {
			if parsed, err := strconv.Atoi(q); err == nil && parsed >= 0 {
				delay = parsed
			}
		}
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}

		if q := r.URL.Query().Get("fail"); q != "" {
			if code, err := strconv.Atoi(q); err == nil && code >= 400 {
				w.Header().Set("Content-Type", "application/json")
				if code == http.StatusTooManyRequests {
					w.Header().Set("Retry-After", "2")
				}
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{"error":"simulated provider failure"}`))
				return
			}
		}

		if failRate > 0 && rand.Float64() < failRate {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(failStatus)
			_, _ = w.Write([]byte(`{"error":"random simulated provider outage"}`))
			return
		}

		var req webhookRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid JSON payload"}`))
			return
		}

		resp := webhookResponse{
			MessageID: uuid.NewString(),
			Status:    "accepted",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}

		logger.Info("accepted notification webhook",
			slog.String("to", req.To),
			slog.String("channel", req.Channel),
			slog.String("message_id", resp.MessageID),
			slog.String("correlation_id", r.Header.Get("X-Correlation-ID")),
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(resp)
	}

	mux.HandleFunc("/webhook", handleWebhook)
	mux.HandleFunc("/", handleWebhook)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("mock webhook provider starting", slog.String("port", port))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("mock provider stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

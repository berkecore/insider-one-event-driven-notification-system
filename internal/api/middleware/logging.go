package middleware

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/go-chi/chi/v5"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

func (sr *statusRecorder) Write(b []byte) (int, error) {
	if sr.status == 0 {
		sr.status = http.StatusOK
	}
	n, err := sr.ResponseWriter.Write(b)
	sr.bytes += n
	return n, err
}

// Hijack preserves http.Hijacker support so WebSocket upgrades succeed cleanly through the middleware.
func (sr *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := sr.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
	}
	return h.Hijack()
}

// Flush preserves http.Flusher support.
func (sr *statusRecorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// RequestLogger logs structured JSON access records with correlation ID and records Prometheus HTTP metrics.
func RequestLogger(logger *slog.Logger, metrics *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			dur := time.Since(start)
			route := r.URL.Path
			if rctx := chi.RouteContext(r.Context()); rctx != nil && rctx.RoutePattern() != "" {
				route = rctx.RoutePattern()
			}

			if metrics != nil {
				metrics.RecordHTTPRequest(r.Method, route, rec.status, dur)
			}

			if logger != nil && r.URL.Path != "/health" && r.URL.Path != "/metrics" {
				observability.LoggerWithContext(r.Context(), logger).Info("http request",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.String("route", route),
					slog.Int("status", rec.status),
					slog.Int("bytes", rec.bytes),
					slog.Float64("duration_ms", float64(dur.Microseconds())/1000.0),
					slog.String("remote_addr", r.RemoteAddr),
				)
			}
		})
	}
}

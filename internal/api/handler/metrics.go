package handler

import (
	"context"
	"net/http"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/dto"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
)

// QueueDepthProvider fetches real-time queue depths from Redis.
type QueueDepthProvider interface {
	Depths(ctx context.Context) (map[string]observability.QueueDepthSnapshot, error)
}

// MetricsHandler serves both Prometheus (/metrics) and real-time JSON (/api/v1/metrics) endpoints.
type MetricsHandler struct {
	metrics *observability.Metrics
	queues  QueueDepthProvider
}

func NewMetricsHandler(metrics *observability.Metrics, queues QueueDepthProvider) *MetricsHandler {
	return &MetricsHandler{
		metrics: metrics,
		queues:  queues,
	}
}

// Prometheus serves standard Prometheus metrics exposition at GET /metrics.
func (h *MetricsHandler) Prometheus(w http.ResponseWriter, r *http.Request) {
	if h.queues != nil {
		if depths, err := h.queues.Depths(r.Context()); err == nil {
			h.metrics.SetQueueDepths(depths)
		}
	}
	h.metrics.Handler().ServeHTTP(w, r)
}

// Summary serves real-time JSON metrics (queue depth, success/failure rates, latency, circuit breakers)
// at GET /api/v1/metrics.
func (h *MetricsHandler) Summary(w http.ResponseWriter, r *http.Request) {
	var depths map[string]observability.QueueDepthSnapshot
	if h.queues != nil {
		if d, err := h.queues.Depths(r.Context()); err == nil {
			depths = d
		}
	}
	snap := h.metrics.Snapshot(depths)
	dto.WriteJSON(w, http.StatusOK, snap)
}

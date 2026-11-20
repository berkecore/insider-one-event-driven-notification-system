package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/dto"
)

// DependencyCheck verifies connectivity to an external backing service (e.g. Postgres or Redis).
type DependencyCheck func(ctx context.Context) error

// HealthHandler serves liveness (/health) and readiness (/ready) probes.
type HealthHandler struct {
	checks map[string]DependencyCheck
}

func NewHealthHandler(checks map[string]DependencyCheck) *HealthHandler {
	return &HealthHandler{checks: checks}
}

// Health handles GET /health (liveness probe).
func (h *HealthHandler) Health(w http.ResponseWriter, _ *http.Request) {
	dto.WriteJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"timestamp": time.Now().UTC(),
	})
}

// Ready handles GET /ready (readiness probe checking PostgreSQL and Redis).
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	allOK := true
	results := make(map[string]string, len(h.checks))
	for name, check := range h.checks {
		if check == nil {
			results[name] = "ok"
			continue
		}
		if err := check(ctx); err != nil {
			allOK = false
			results[name] = "unhealthy: " + err.Error()
		} else {
			results[name] = "ok"
		}
	}

	status := http.StatusOK
	overall := "ready"
	if !allOK {
		status = http.StatusServiceUnavailable
		overall = "not_ready"
	}

	dto.WriteJSON(w, status, map[string]any{
		"status":    overall,
		"timestamp": time.Now().UTC(),
		"checks":    results,
	})
}

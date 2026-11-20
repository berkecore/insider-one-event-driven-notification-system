package middleware

import (
	"net/http"
	"strings"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/google/uuid"
)

const (
	HeaderCorrelationID = "X-Correlation-ID"
	HeaderRequestID     = "X-Request-ID"
)

// CorrelationID ensures every incoming HTTP request carries a correlation ID in context and response headers.
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := strings.TrimSpace(r.Header.Get(HeaderCorrelationID))
		if cid == "" {
			cid = strings.TrimSpace(r.Header.Get(HeaderRequestID))
		}
		if cid == "" {
			cid = uuid.NewString()
		}
		w.Header().Set(HeaderCorrelationID, cid)
		ctx := observability.WithCorrelationID(r.Context(), cid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

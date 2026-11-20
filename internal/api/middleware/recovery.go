package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/dto"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
)

// Recovery catches panics in HTTP handlers, logs the stack trace, and returns JSON 500.
func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if logger != nil {
						observability.LoggerWithContext(r.Context(), logger).Error("http handler panic recovered",
							slog.Any("panic", rec),
							slog.String("stack", string(debug.Stack())),
						)
					}
					dto.WriteError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error", nil)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

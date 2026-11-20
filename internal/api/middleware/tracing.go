package middleware

import (
	"net/http"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Tracing extracts W3C TraceContext from incoming headers and starts an OpenTelemetry server span.
func Tracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := observability.Tracer().Start(ctx, r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.target", r.URL.Path),
			),
		)
		defer span.End()

		if cid := observability.CorrelationIDFromContext(ctx); cid != "" {
			span.SetAttributes(attribute.String("correlation.id", cid))
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

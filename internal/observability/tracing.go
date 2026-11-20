package observability

import (
	"context"
	"fmt"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerName is the instrumentation scope used across the notification service.
const TracerName = "github.com/berkecore/insider-one-event-driven-notification-system"

// InitTracing configures the global OpenTelemetry TracerProvider and W3C propagator.
// Call the returned shutdown function during graceful termination.
func InitTracing(ctx context.Context, cfg config.TracingConfig) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.Exporter == "" || cfg.Exporter == "noop" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	var (
		exporter sdktrace.SpanExporter
		err      error
	)

	switch cfg.Exporter {
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	case "otlp":
		exporter, err = otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
			otlptracehttp.WithInsecure(),
		)
	default:
		return nil, fmt.Errorf("unsupported OTEL_EXPORTER %q", cfg.Exporter)
	}
	if err != nil {
		return nil, fmt.Errorf("init trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Tracer returns the service's OpenTelemetry tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// InjectTraceContext serializes the active W3C trace context into a string map
// for propagation through PostgreSQL outbox rows and Redis queue payloads.
func InjectTraceContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	out := make(map[string]string, len(carrier))
	for k, v := range carrier {
		out[k] = v
	}
	return out
}

// ExtractTraceContext restores a W3C trace context from a serialized string map.
func ExtractTraceContext(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

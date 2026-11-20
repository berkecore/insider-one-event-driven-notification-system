package api

import (
	"log/slog"
	"net/http"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/handler"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/middleware"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/realtime"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"github.com/go-chi/chi/v5"
)

// RouterDeps bundles all dependencies required by the HTTP API server.
type RouterDeps struct {
	NotificationSvc *service.NotificationService
	TemplateSvc     *service.TemplateService
	Metrics         *observability.Metrics
	QueueDepths     handler.QueueDepthProvider
	HealthChecks    map[string]handler.DependencyCheck
	Hub             *realtime.Hub
	Logger          *slog.Logger
}

// NewRouter constructs the chi HTTP router with observability middlewares and all REST/WS/Doc routes.
func NewRouter(deps RouterDeps) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.CorrelationID)
	r.Use(middleware.Tracing)
	r.Use(middleware.RequestLogger(deps.Logger, deps.Metrics))
	r.Use(middleware.Recovery(deps.Logger))

	healthH := handler.NewHealthHandler(deps.HealthChecks)
	metricsH := handler.NewMetricsHandler(deps.Metrics, deps.QueueDepths)
	docsH := handler.NewDocsHandler()
	wsH := handler.NewWebSocketHandler(deps.Hub)
	notifH := handler.NewNotificationHandler(deps.NotificationSvc)
	tplH := handler.NewTemplateHandler(deps.TemplateSvc)

	// Observability & Docs
	r.Get("/health", healthH.Health)
	r.Get("/ready", healthH.Ready)
	r.Get("/metrics", metricsH.Prometheus)
	r.Get("/openapi.yaml", docsH.Spec)
	r.Get("/docs", docsH.UI)
	r.Get("/swagger", func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "/docs", http.StatusTemporaryRedirect)
	})

	// Real-time WebSocket stream
	r.Get("/ws/notifications", wsH.Stream)

	// Versioned REST API
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/metrics", metricsH.Summary)

		r.Route("/notifications", func(r chi.Router) {
			r.Post("/", notifH.Create)
			r.Get("/", notifH.List)
			r.Post("/batch", notifH.CreateBatch)
			r.Get("/batches/{batchId}", notifH.GetBatchByID)
			r.Get("/{id}", notifH.GetByID)
			r.Post("/{id}/cancel", notifH.Cancel)
			r.Post("/{id}/retry", notifH.Replay)
		})

		if deps.TemplateSvc != nil {
			r.Route("/templates", func(r chi.Router) {
				r.Post("/", tplH.Create)
				r.Get("/", tplH.List)
				r.Get("/{id}", tplH.GetByID)
				r.Put("/{id}", tplH.Update)
				r.Delete("/{id}", tplH.Delete)
				r.Post("/{id}/preview", tplH.Preview)
			})
		}
	})

	return r
}

package handler

import (
	"net/http"
	"strings"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/dto"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/realtime"
	"github.com/google/uuid"
)

// WebSocketHandler upgrades connections to receive real-time notification status transitions.
type WebSocketHandler struct {
	hub *realtime.Hub
}

func NewWebSocketHandler(hub *realtime.Hub) *WebSocketHandler {
	return &WebSocketHandler{hub: hub}
}

// Stream handles GET /ws/notifications with optional ?notification_id=, ?batch_id=, and ?channel= filters.
func (h *WebSocketHandler) Stream(w http.ResponseWriter, r *http.Request) {
	if h.hub == nil {
		dto.WriteError(w, r, http.StatusServiceUnavailable, "WS_UNAVAILABLE", "real-time hub is not enabled", nil)
		return
	}

	q := r.URL.Query()
	var filter realtime.SubscriptionFilter

	if v := strings.TrimSpace(q.Get("notification_id")); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "notification_id must be a valid UUID", nil)
			return
		}
		filter.NotificationID = &id
	}
	if v := strings.TrimSpace(q.Get("batch_id")); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "batch_id must be a valid UUID", nil)
			return
		}
		filter.BatchID = &id
	}
	if v := strings.TrimSpace(q.Get("channel")); v != "" {
		ch := domain.Channel(strings.ToLower(v))
		if !ch.IsValid() {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "channel must be sms, email, or push", nil)
			return
		}
		filter.Channel = &ch
	}

	h.hub.ServeWS(w, r, filter)
}

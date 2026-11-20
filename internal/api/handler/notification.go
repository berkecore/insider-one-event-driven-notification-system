package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/dto"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

const (
	maxSingleBodyBytes = 1 << 20  // 1 MiB
	maxBatchBodyBytes  = 16 << 20 // 16 MiB (supports 1,000 large email notifications)
)

// NotificationHandler exposes REST endpoints for notification lifecycle management.
type NotificationHandler struct {
	svc *service.NotificationService
}

func NewNotificationHandler(svc *service.NotificationService) *NotificationHandler {
	return &NotificationHandler{svc: svc}
}

// Create handles POST /api/v1/notifications.
func (h *NotificationHandler) Create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSingleBodyBytes)
	var req dto.CreateNotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body: "+err.Error(), nil)
		return
	}

	headerIdem := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	res, err := h.svc.Create(r.Context(), req.ToServiceInput(headerIdem))
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}

	status := http.StatusCreated
	if res.Replayed {
		status = http.StatusOK
		w.Header().Set("X-Idempotent-Replayed", "true")
	}
	dto.WriteJSON(w, status, res.Notification)
}

// CreateBatch handles POST /api/v1/notifications/batch (up to 1,000 notifications).
func (h *NotificationHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBodyBytes)
	var req dto.CreateBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body: "+err.Error(), nil)
		return
	}

	items := make([]service.CreateNotificationInput, len(req.Notifications))
	for i, n := range req.Notifications {
		items[i] = n.ToServiceInput("")
	}

	res, err := h.svc.CreateBatch(r.Context(), service.CreateBatchInput{
		Items:          items,
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
	})
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}

	status := http.StatusCreated
	if res.Replayed {
		status = http.StatusOK
		w.Header().Set("X-Idempotent-Replayed", "true")
	}
	dto.WriteJSON(w, status, res.Summary)
}

// GetByID handles GET /api/v1/notifications/{id}.
func (h *NotificationHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "notification id must be a valid UUID", nil)
		return
	}

	n, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, n)
}

// GetBatchByID handles GET /api/v1/notifications/batches/{batchId}.
func (h *NotificationHandler) GetBatchByID(w http.ResponseWriter, r *http.Request) {
	batchID, err := uuid.Parse(chi.URLParam(r, "batchId"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "batchId must be a valid UUID", nil)
		return
	}

	summary, err := h.svc.GetBatchByID(r.Context(), batchID)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, summary)
}

// List handles GET /api/v1/notifications with filtering and pagination.
func (h *NotificationHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := domain.NotificationFilter{
		Page:     1,
		PageSize: 20,
	}

	if v := strings.TrimSpace(q.Get("status")); v != "" {
		st := domain.Status(strings.ToLower(v))
		if !st.IsValid() {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "invalid status filter", nil)
			return
		}
		filter.Status = &st
	}

	if v := strings.TrimSpace(q.Get("channel")); v != "" {
		ch := domain.Channel(strings.ToLower(v))
		if !ch.IsValid() {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "invalid channel filter", nil)
			return
		}
		filter.Channel = &ch
	}

	if v := strings.TrimSpace(q.Get("priority")); v != "" {
		pr := domain.Priority(strings.ToLower(v))
		if !pr.IsValid() {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "invalid priority filter", nil)
			return
		}
		filter.Priority = &pr
	}

	if v := strings.TrimSpace(q.Get("batch_id")); v != "" {
		bid, err := uuid.Parse(v)
		if err != nil {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "batch_id must be a valid UUID", nil)
			return
		}
		filter.BatchID = &bid
	}

	if v := strings.TrimSpace(q.Get("from")); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "from must be an RFC3339 timestamp", nil)
			return
		}
		filter.From = &t
	}

	if v := strings.TrimSpace(q.Get("to")); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "to must be an RFC3339 timestamp", nil)
			return
		}
		filter.To = &t
	}

	if v := strings.TrimSpace(q.Get("page")); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "page must be a positive integer", nil)
			return
		}
		filter.Page = p
	}

	if v := strings.TrimSpace(q.Get("page_size")); v != "" {
		ps, err := strconv.Atoi(v)
		if err != nil || ps < 1 || ps > 100 {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "page_size must be between 1 and 100", nil)
			return
		}
		filter.PageSize = ps
	}

	page, err := h.svc.List(r.Context(), filter)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, page)
}

// Cancel handles POST /api/v1/notifications/{id}/cancel.
func (h *NotificationHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "notification id must be a valid UUID", nil)
		return
	}

	n, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, n)
}

// Replay handles POST /api/v1/notifications/{id}/retry to re-enqueue a permanently failed notification.
func (h *NotificationHandler) Replay(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "notification id must be a valid UUID", nil)
		return
	}

	n, err := h.svc.Replay(r.Context(), id)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, n)
}

package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/dto"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// TemplateHandler exposes CRUD and preview endpoints for notification templates.
type TemplateHandler struct {
	svc *service.TemplateService
}

func NewTemplateHandler(svc *service.TemplateService) *TemplateHandler {
	return &TemplateHandler{svc: svc}
}

func (h *TemplateHandler) Create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSingleBodyBytes)
	var req dto.TemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body: "+err.Error(), nil)
		return
	}

	tpl, err := h.svc.Create(r.Context(), service.TemplateInput{
		Name:    req.Name,
		Channel: domain.Channel(req.Channel),
		Body:    req.Body,
	})
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusCreated, tpl)
}

func (h *TemplateHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "template id must be a valid UUID", nil)
		return
	}
	tpl, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, tpl)
}

func (h *TemplateHandler) List(w http.ResponseWriter, r *http.Request) {
	var ch *domain.Channel
	if v := strings.TrimSpace(r.URL.Query().Get("channel")); v != "" {
		c := domain.Channel(strings.ToLower(v))
		if !c.IsValid() {
			dto.WriteError(w, r, http.StatusBadRequest, "INVALID_FILTER", "invalid channel filter", nil)
			return
		}
		ch = &c
	}

	items, err := h.svc.List(r.Context(), ch)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *TemplateHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "template id must be a valid UUID", nil)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxSingleBodyBytes)
	var req dto.TemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body: "+err.Error(), nil)
		return
	}

	tpl, err := h.svc.Update(r.Context(), id, service.TemplateInput{
		Name:    req.Name,
		Channel: domain.Channel(req.Channel),
		Body:    req.Body,
	})
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, tpl)
}

func (h *TemplateHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "template id must be a valid UUID", nil)
		return
	}
	if err := h.svc.Delete(r.Context(), id); err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *TemplateHandler) Preview(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_ID", "template id must be a valid UUID", nil)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxSingleBodyBytes)
	var req dto.PreviewTemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dto.WriteError(w, r, http.StatusBadRequest, "INVALID_JSON", "invalid request body: "+err.Error(), nil)
		return
	}

	rendered, err := h.svc.Preview(r.Context(), id, req.Variables)
	if err != nil {
		dto.WriteDomainError(w, r, err)
		return
	}
	dto.WriteJSON(w, http.StatusOK, map[string]string{"rendered": rendered})
}

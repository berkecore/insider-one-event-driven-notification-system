package dto

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"github.com/google/uuid"
)

// CreateNotificationRequest is the JSON body for POST /api/v1/notifications.
type CreateNotificationRequest struct {
	Recipient      string            `json:"recipient"`
	Channel        string            `json:"channel"`
	Content        string            `json:"content"`
	Priority       string            `json:"priority,omitempty"`
	TemplateID     *uuid.UUID        `json:"template_id,omitempty"`
	TemplateVars   map[string]string `json:"template_vars,omitempty"`
	ScheduledAt    *time.Time        `json:"scheduled_at,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}

func (r CreateNotificationRequest) ToServiceInput(headerIdemKey string) service.CreateNotificationInput {
	idem := r.IdempotencyKey
	if idem == "" {
		idem = headerIdemKey
	}
	return service.CreateNotificationInput{
		Recipient:      r.Recipient,
		Channel:        domain.Channel(r.Channel),
		Content:        r.Content,
		Priority:       domain.Priority(r.Priority),
		TemplateID:     r.TemplateID,
		TemplateVars:   r.TemplateVars,
		ScheduledAt:    r.ScheduledAt,
		IdempotencyKey: idem,
	}
}

// CreateBatchRequest is the JSON body for POST /api/v1/notifications/batch.
type CreateBatchRequest struct {
	Notifications []CreateNotificationRequest `json:"notifications"`
}

// TemplateRequest is the JSON body for creating or updating a template.
type TemplateRequest struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

// PreviewTemplateRequest is the JSON body for POST /api/v1/templates/{id}/preview.
type PreviewTemplateRequest struct {
	Variables map[string]string `json:"variables"`
}

// ErrorResponse provides uniform structured JSON error output across all endpoints.
type ErrorResponse struct {
	Code          string              `json:"code"`
	Message       string              `json:"message"`
	CorrelationID string              `json:"correlation_id,omitempty"`
	Details       []domain.FieldError `json:"details,omitempty"`
}

// WriteJSON encodes payload as JSON with the given HTTP status code.
func WriteJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

// WriteError writes a structured JSON error response including correlation_id.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, message string, details []domain.FieldError) {
	WriteJSON(w, status, ErrorResponse{
		Code:          code,
		Message:       message,
		CorrelationID: observability.CorrelationIDFromContext(r.Context()),
		Details:       details,
	})
}

// WriteDomainError maps domain sentinel and validation errors to appropriate HTTP status codes.
func WriteDomainError(w http.ResponseWriter, r *http.Request, err error) {
	var verr *domain.ValidationError
	if errors.As(err, &verr) {
		WriteError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "request validation failed", verr.Fields)
		return
	}

	switch {
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrTemplateNotFound):
		WriteError(w, r, http.StatusNotFound, "NOT_FOUND", err.Error(), nil)
	case errors.Is(err, domain.ErrBatchEmpty), errors.Is(err, domain.ErrBatchTooLarge):
		WriteError(w, r, http.StatusBadRequest, "INVALID_BATCH", err.Error(), nil)
	case errors.Is(err, domain.ErrIdempotencyConflict):
		WriteError(w, r, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error(), nil)
	case errors.Is(err, domain.ErrNotCancellable):
		WriteError(w, r, http.StatusConflict, "NOT_CANCELLABLE", err.Error(), nil)
	case errors.Is(err, domain.ErrNotReplayable):
		WriteError(w, r, http.StatusConflict, "NOT_REPLAYABLE", err.Error(), nil)
	case errors.Is(err, domain.ErrTemplateNameExists), errors.Is(err, domain.ErrConflict):
		WriteError(w, r, http.StatusConflict, "CONFLICT", err.Error(), nil)
	default:
		WriteError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred", nil)
	}
}

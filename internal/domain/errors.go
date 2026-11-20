package domain

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound            = errors.New("resource not found")
	ErrConflict            = errors.New("resource conflict")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request payload")
	ErrInvalidTransition   = errors.New("invalid notification status transition")
	ErrNotCancellable      = errors.New("notification cannot be cancelled in its current state")
	ErrNotReplayable       = errors.New("only permanently failed notifications can be replayed")
	ErrBatchTooLarge       = errors.New("batch exceeds maximum limit of 1000 notifications")
	ErrBatchEmpty          = errors.New("batch must contain at least one notification")
	ErrTemplateNotFound    = errors.New("template not found")
	ErrTemplateNameExists  = errors.New("template with this name already exists")
)

// FieldError describes a single validation failure, optionally tied to a batch item index.
type FieldError struct {
	Index   *int   `json:"index,omitempty"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError aggregates one or more field validation failures.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	if len(e.Fields) == 0 {
		return "validation failed"
	}
	parts := make([]string, 0, len(e.Fields))
	for _, f := range e.Fields {
		if f.Index != nil {
			parts = append(parts, fmt.Sprintf("items[%d].%s: %s", *f.Index, f.Field, f.Message))
		} else {
			parts = append(parts, fmt.Sprintf("%s: %s", f.Field, f.Message))
		}
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func (e *ValidationError) Add(field, message string) {
	e.Fields = append(e.Fields, FieldError{Field: field, Message: message})
}

func (e *ValidationError) AddAt(index int, field, message string) {
	idx := index
	e.Fields = append(e.Fields, FieldError{Index: &idx, Field: field, Message: message})
}

func (e *ValidationError) HasErrors() bool {
	return len(e.Fields) > 0
}

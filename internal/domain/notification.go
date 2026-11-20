package domain

import (
	"time"

	"github.com/google/uuid"
)

// Channel represents a supported notification delivery channel.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// AllChannels lists every supported delivery channel.
var AllChannels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

func (c Channel) IsValid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	default:
		return false
	}
}

// Priority determines dequeue precedence inside each channel queue.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// AllPriorities lists priorities from highest to lowest.
var AllPriorities = []Priority{PriorityHigh, PriorityNormal, PriorityLow}

func (p Priority) IsValid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	default:
		return false
	}
}

// Status represents the lifecycle state of a notification.
type Status string

const (
	StatusScheduled  Status = "scheduled"
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusDelivered  Status = "delivered"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

func (s Status) IsValid() bool {
	switch s {
	case StatusScheduled, StatusPending, StatusQueued, StatusProcessing,
		StatusRetrying, StatusDelivered, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// CanCancel returns true if a notification in this status can still be cancelled.
func (s Status) CanCancel() bool {
	switch s {
	case StatusScheduled, StatusPending, StatusQueued, StatusRetrying:
		return true
	default:
		return false
	}
}

// IsTerminal returns true if no further automatic transitions occur from this status.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusDelivered, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// Notification is the core aggregate tracked across ingestion, queueing, and delivery.
type Notification struct {
	ID                  uuid.UUID         `json:"id"`
	BatchID             *uuid.UUID        `json:"batch_id,omitempty"`
	IdempotencyKey      *string           `json:"idempotency_key,omitempty"`
	RequestHash         string            `json:"-"`
	Recipient           string            `json:"recipient"`
	Channel             Channel           `json:"channel"`
	Content             string            `json:"content"`
	Priority            Priority          `json:"priority"`
	Status              Status            `json:"status"`
	TemplateID          *uuid.UUID        `json:"template_id,omitempty"`
	CorrelationID       string            `json:"correlation_id,omitempty"`
	TraceContext        map[string]string `json:"-"`
	ScheduledAt         *time.Time        `json:"scheduled_at,omitempty"`
	AttemptCount        int               `json:"attempt_count"`
	MaxAttempts         int               `json:"max_attempts"`
	NextRetryAt         *time.Time        `json:"next_retry_at,omitempty"`
	LastError           *string           `json:"last_error,omitempty"`
	ProviderMessageID   *string           `json:"provider_message_id,omitempty"`
	ProcessingStartedAt *time.Time        `json:"processing_started_at,omitempty"`
	DeliveredAt         *time.Time        `json:"delivered_at,omitempty"`
	CancelledAt         *time.Time        `json:"cancelled_at,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	Attempts            []DeliveryAttempt `json:"attempts,omitempty"`
}

// NotificationBatch groups up to 1000 notifications created in a single batch request.
type NotificationBatch struct {
	ID             uuid.UUID  `json:"id"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
	RequestHash    string     `json:"-"`
	TotalCount     int        `json:"total_count"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// BatchSummary aggregates status counts and notifications belonging to a batch.
type BatchSummary struct {
	Batch         NotificationBatch `json:"batch"`
	StatusCounts  map[Status]int    `json:"status_counts"`
	Notifications []Notification    `json:"notifications"`
}

// DeliveryAttempt records an immutable audit trail entry for every external provider call.
type DeliveryAttempt struct {
	ID                int64     `json:"id"`
	NotificationID    uuid.UUID `json:"notification_id"`
	AttemptNumber     int       `json:"attempt_number"`
	StatusCode        int       `json:"status_code,omitempty"`
	ProviderMessageID *string   `json:"provider_message_id,omitempty"`
	ErrorMessage      *string   `json:"error_message,omitempty"`
	Retryable         bool      `json:"retryable"`
	DurationMs        int64     `json:"duration_ms"`
	CreatedAt         time.Time `json:"created_at"`
}

// OutboxEvent represents an entry in the PostgreSQL transactional outbox awaiting Redis enqueue.
type OutboxEvent struct {
	ID             int64             `json:"id"`
	NotificationID uuid.UUID         `json:"notification_id"`
	Channel        Channel           `json:"channel"`
	Priority       Priority          `json:"priority"`
	CorrelationID  string            `json:"correlation_id"`
	TraceContext   map[string]string `json:"trace_context,omitempty"`
	PublishedAt    *time.Time        `json:"published_at,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
}

// StatusEvent is published over Redis Pub/Sub and fanned out to WebSocket subscribers.
type StatusEvent struct {
	NotificationID    uuid.UUID  `json:"notification_id"`
	BatchID           *uuid.UUID `json:"batch_id,omitempty"`
	Channel           Channel    `json:"channel"`
	Priority          Priority   `json:"priority"`
	Status            Status     `json:"status"`
	AttemptCount      int        `json:"attempt_count"`
	ProviderMessageID *string    `json:"provider_message_id,omitempty"`
	LastError         *string    `json:"last_error,omitempty"`
	CorrelationID     string     `json:"correlation_id,omitempty"`
	Timestamp         time.Time  `json:"timestamp"`
}

// NotificationFilter captures query parameters for listing notifications.
type NotificationFilter struct {
	Status   *Status
	Channel  *Channel
	Priority *Priority
	BatchID  *uuid.UUID
	From     *time.Time
	To       *time.Time
	Page     int
	PageSize int
}

// NotificationPage holds a paginated slice of notifications and pagination metadata.
type NotificationPage struct {
	Items      []Notification `json:"items"`
	Total      int64          `json:"total"`
	Page       int            `json:"page"`
	PageSize   int            `json:"page_size"`
	TotalPages int            `json:"total_pages"`
}

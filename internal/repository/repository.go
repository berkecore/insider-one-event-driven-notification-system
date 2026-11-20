package repository

import (
	"context"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
)

// NotificationRepository defines persistence operations for notifications and batches.
type NotificationRepository interface {
	CreateWithOutbox(ctx context.Context, n *domain.Notification) error
	CreateBatchWithOutbox(ctx context.Context, batch *domain.NotificationBatch, items []*domain.Notification) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error)
	GetBatchByID(ctx context.Context, batchID uuid.UUID) (*domain.BatchSummary, error)
	GetBatchByIdempotencyKey(ctx context.Context, key string) (*domain.BatchSummary, error)
	List(ctx context.Context, filter domain.NotificationFilter) (*domain.NotificationPage, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	Replay(ctx context.Context, id uuid.UUID) (*domain.Notification, error)

	// Worker lifecycle operations
	ClaimForProcessing(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	MarkDelivered(ctx context.Context, id uuid.UUID, providerMessageID string) (*domain.Notification, error)
	MarkRetrying(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, lastErr string) (*domain.Notification, error)
	MarkDeferred(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, lastErr string) (*domain.Notification, error)
	MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) (*domain.Notification, error)
	PromoteScheduled(ctx context.Context, limit int) ([]domain.Notification, error)
	ReclaimStaleProcessing(ctx context.Context, leaseDuration time.Duration, limit int) (int64, error)
}

// OutboxRepository processes pending transactional outbox events atomically.
type OutboxRepository interface {
	ProcessPending(ctx context.Context, limit int, publishFn func(events []domain.OutboxEvent) error) ([]domain.OutboxEvent, error)
}

// AttemptRepository records delivery attempts for observability and auditing.
type AttemptRepository interface {
	Record(ctx context.Context, attempt *domain.DeliveryAttempt) error
	ListByNotificationID(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error)
}

// TemplateRepository manages reusable message templates.
type TemplateRepository interface {
	Create(ctx context.Context, tpl *domain.Template) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Template, error)
	List(ctx context.Context, channel *domain.Channel) ([]domain.Template, error)
	Update(ctx context.Context, tpl *domain.Template) error
	Delete(ctx context.Context, id uuid.UUID) error
}

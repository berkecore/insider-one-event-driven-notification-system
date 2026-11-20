package postgres

import (
	"context"
	"fmt"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AttemptRepo stores immutable delivery attempt audit entries.
type AttemptRepo struct {
	pool *pgxpool.Pool
}

func NewAttemptRepo(pool *pgxpool.Pool) *AttemptRepo {
	return &AttemptRepo{pool: pool}
}

func (r *AttemptRepo) Record(ctx context.Context, a *domain.DeliveryAttempt) error {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO notification_attempts (
			notification_id, attempt_number, status_code, provider_message_id,
			error_message, retryable, duration_ms
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at
	`, a.NotificationID, a.AttemptNumber, a.StatusCode, a.ProviderMessageID,
		a.ErrorMessage, a.Retryable, a.DurationMs,
	).Scan(&a.ID, &a.CreatedAt)
	if err != nil {
		return fmt.Errorf("record delivery attempt: %w", err)
	}
	return nil
}

func (r *AttemptRepo) ListByNotificationID(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryAttempt, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, notification_id, attempt_number, status_code, provider_message_id,
		       error_message, retryable, duration_ms, created_at
		FROM notification_attempts
		WHERE notification_id = $1
		ORDER BY attempt_number ASC, id ASC
	`, notificationID)
	if err != nil {
		return nil, fmt.Errorf("list delivery attempts: %w", err)
	}
	defer rows.Close()

	var attempts []domain.DeliveryAttempt
	for rows.Next() {
		var a domain.DeliveryAttempt
		if err := rows.Scan(
			&a.ID, &a.NotificationID, &a.AttemptNumber, &a.StatusCode,
			&a.ProviderMessageID, &a.ErrorMessage, &a.Retryable, &a.DurationMs, &a.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan delivery attempt: %w", err)
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

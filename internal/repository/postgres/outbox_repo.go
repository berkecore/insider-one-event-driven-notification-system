package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxRepo polls unpublished outbox records using FOR UPDATE SKIP LOCKED.
type OutboxRepo struct {
	pool *pgxpool.Pool
}

func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo {
	return &OutboxRepo{pool: pool}
}

// ProcessPending locks up to limit unpublished outbox rows, invokes publishFn, and if publishFn
// succeeds marks them published and transitions pending notifications to queued in the same TX.
func (r *OutboxRepo) ProcessPending(ctx context.Context, limit int, publishFn func(events []domain.OutboxEvent) error) ([]domain.OutboxEvent, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, notification_id, channel, priority, correlation_id, trace_context, created_at
		FROM notification_outbox
		WHERE published_at IS NULL
		ORDER BY id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query outbox: %w", err)
	}

	var events []domain.OutboxEvent
	for rows.Next() {
		var (
			ev       domain.OutboxEvent
			ch       string
			prio     string
			traceRaw []byte
		)
		if err := rows.Scan(&ev.ID, &ev.NotificationID, &ch, &prio, &ev.CorrelationID, &traceRaw, &ev.CreatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		ev.Channel = domain.Channel(ch)
		ev.Priority = domain.Priority(prio)
		if len(traceRaw) > 0 {
			_ = json.Unmarshal(traceRaw, &ev.TraceContext)
		}
		events = append(events, ev)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}

	if err := publishFn(events); err != nil {
		return nil, err
	}

	outboxIDs := make([]int64, len(events))
	notifIDs := make([]uuid.UUID, len(events))
	for i, ev := range events {
		outboxIDs[i] = ev.ID
		notifIDs[i] = ev.NotificationID
	}

	if _, err := tx.Exec(ctx, `
		UPDATE notification_outbox
		SET published_at = NOW()
		WHERE id = ANY($1)
	`, outboxIDs); err != nil {
		return nil, fmt.Errorf("mark outbox published: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE notifications
		SET status = 'queued', updated_at = NOW()
		WHERE id = ANY($1) AND status = 'pending'
	`, notifIDs); err != nil {
		return nil, fmt.Errorf("mark notifications queued: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox tx: %w", err)
	}
	return events, nil
}

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NotificationRepo implements repository.NotificationRepository on PostgreSQL.
type NotificationRepo struct {
	pool *pgxpool.Pool
}

func NewNotificationRepo(pool *pgxpool.Pool) *NotificationRepo {
	return &NotificationRepo{pool: pool}
}

const notificationSelectColumns = `
	id, batch_id, idempotency_key, request_hash, recipient, channel, content, priority,
	status, template_id, correlation_id, trace_context, scheduled_at, attempt_count, max_attempts,
	next_retry_at, last_error, provider_message_id, processing_started_at, delivered_at, cancelled_at,
	created_at, updated_at
`

func scanNotification(row pgx.Row) (*domain.Notification, error) {
	var (
		n            domain.Notification
		channel      string
		priority     string
		status       string
		traceRaw     []byte
	)
	err := row.Scan(
		&n.ID, &n.BatchID, &n.IdempotencyKey, &n.RequestHash, &n.Recipient, &channel, &n.Content, &priority,
		&status, &n.TemplateID, &n.CorrelationID, &traceRaw, &n.ScheduledAt, &n.AttemptCount, &n.MaxAttempts,
		&n.NextRetryAt, &n.LastError, &n.ProviderMessageID, &n.ProcessingStartedAt, &n.DeliveredAt, &n.CancelledAt,
		&n.CreatedAt, &n.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	n.Channel = domain.Channel(channel)
	n.Priority = domain.Priority(priority)
	n.Status = domain.Status(status)
	if len(traceRaw) > 0 {
		_ = json.Unmarshal(traceRaw, &n.TraceContext)
	}
	return &n, nil
}

func marshalTraceContext(tc map[string]string) []byte {
	if len(tc) == 0 {
		return nil
	}
	b, err := json.Marshal(tc)
	if err != nil {
		return nil
	}
	return b
}

func (r *NotificationRepo) CreateWithOutbox(ctx context.Context, n *domain.Notification) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	traceJSON := marshalTraceContext(n.TraceContext)
	err = tx.QueryRow(ctx, `
		INSERT INTO notifications (
			id, batch_id, idempotency_key, request_hash, recipient, channel, content, priority,
			status, template_id, correlation_id, trace_context, scheduled_at, max_attempts
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		RETURNING created_at, updated_at
	`,
		n.ID, n.BatchID, n.IdempotencyKey, n.RequestHash, n.Recipient, string(n.Channel), n.Content, string(n.Priority),
		string(n.Status), n.TemplateID, n.CorrelationID, traceJSON, n.ScheduledAt, n.MaxAttempts,
	).Scan(&n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrConflict
		}
		return fmt.Errorf("insert notification: %w", err)
	}

	if n.Status == domain.StatusPending {
		if _, err := tx.Exec(ctx, `
			INSERT INTO notification_outbox (notification_id, channel, priority, correlation_id, trace_context)
			VALUES ($1, $2, $3, $4, $5)
		`, n.ID, string(n.Channel), string(n.Priority), n.CorrelationID, traceJSON); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (r *NotificationRepo) CreateBatchWithOutbox(ctx context.Context, batch *domain.NotificationBatch, items []*domain.Notification) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin batch tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	err = tx.QueryRow(ctx, `
		INSERT INTO notification_batches (id, idempotency_key, request_hash, total_count)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at, updated_at
	`, batch.ID, batch.IdempotencyKey, batch.RequestHash, batch.TotalCount).Scan(&batch.CreatedAt, &batch.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrConflict
		}
		return fmt.Errorf("insert batch: %w", err)
	}

	pgBatch := &pgx.Batch{}
	for _, n := range items {
		traceJSON := marshalTraceContext(n.TraceContext)
		pgBatch.Queue(`
			INSERT INTO notifications (
				id, batch_id, idempotency_key, request_hash, recipient, channel, content, priority,
				status, template_id, correlation_id, trace_context, scheduled_at, max_attempts
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		`,
			n.ID, n.BatchID, n.IdempotencyKey, n.RequestHash, n.Recipient, string(n.Channel), n.Content, string(n.Priority),
			string(n.Status), n.TemplateID, n.CorrelationID, traceJSON, n.ScheduledAt, n.MaxAttempts,
		)
		if n.Status == domain.StatusPending {
			pgBatch.Queue(`
				INSERT INTO notification_outbox (notification_id, channel, priority, correlation_id, trace_context)
				VALUES ($1, $2, $3, $4, $5)
			`, n.ID, string(n.Channel), string(n.Priority), n.CorrelationID, traceJSON)
		}
	}

	br := tx.SendBatch(ctx, pgBatch)
	for i := 0; i < pgBatch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			if isUniqueViolation(err) {
				return domain.ErrConflict
			}
			return fmt.Errorf("batch exec item %d: %w", i, err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("close batch results: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch tx: %w", err)
	}

	now := batch.CreatedAt
	for _, n := range items {
		n.CreatedAt = now
		n.UpdatedAt = now
	}
	return nil
}

func (r *NotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+notificationSelectColumns+` FROM notifications WHERE id = $1`, id)
	n, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get notification by id: %w", err)
	}

	attempts, err := NewAttemptRepo(r.pool).ListByNotificationID(ctx, id)
	if err != nil {
		return nil, err
	}
	n.Attempts = attempts
	return n, nil
}

func (r *NotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+notificationSelectColumns+` FROM notifications WHERE idempotency_key = $1`, key)
	n, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get notification by idempotency key: %w", err)
	}
	return n, nil
}

func (r *NotificationRepo) GetBatchByID(ctx context.Context, batchID uuid.UUID) (*domain.BatchSummary, error) {
	var b domain.NotificationBatch
	err := r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, request_hash, total_count, created_at, updated_at
		FROM notification_batches WHERE id = $1
	`, batchID).Scan(&b.ID, &b.IdempotencyKey, &b.RequestHash, &b.TotalCount, &b.CreatedAt, &b.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get batch: %w", err)
	}
	return r.buildBatchSummary(ctx, b)
}

func (r *NotificationRepo) GetBatchByIdempotencyKey(ctx context.Context, key string) (*domain.BatchSummary, error) {
	var b domain.NotificationBatch
	err := r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, request_hash, total_count, created_at, updated_at
		FROM notification_batches WHERE idempotency_key = $1
	`, key).Scan(&b.ID, &b.IdempotencyKey, &b.RequestHash, &b.TotalCount, &b.CreatedAt, &b.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get batch by idempotency key: %w", err)
	}
	return r.buildBatchSummary(ctx, b)
}

func (r *NotificationRepo) buildBatchSummary(ctx context.Context, b domain.NotificationBatch) (*domain.BatchSummary, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+notificationSelectColumns+`
		FROM notifications
		WHERE batch_id = $1
		ORDER BY created_at ASC, id ASC
	`, b.ID)
	if err != nil {
		return nil, fmt.Errorf("list batch notifications: %w", err)
	}
	defer rows.Close()

	counts := make(map[domain.Status]int)
	items := make([]domain.Notification, 0, b.TotalCount)
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("scan batch notification: %w", err)
		}
		counts[n.Status]++
		items = append(items, *n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &domain.BatchSummary{
		Batch:         b,
		StatusCounts:  counts,
		Notifications: items,
	}, nil
}

func (r *NotificationRepo) List(ctx context.Context, filter domain.NotificationFilter) (*domain.NotificationPage, error) {
	page := filter.Page
	if page < 1 {
		page = 1
	}
	pageSize := filter.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	var (
		conds []string
		args  []any
	)
	addCond := func(expr string, val any) {
		args = append(args, val)
		conds = append(conds, fmt.Sprintf(expr, len(args)))
	}

	if filter.Status != nil {
		addCond("status = $%d", string(*filter.Status))
	}
	if filter.Channel != nil {
		addCond("channel = $%d", string(*filter.Channel))
	}
	if filter.Priority != nil {
		addCond("priority = $%d", string(*filter.Priority))
	}
	if filter.BatchID != nil {
		addCond("batch_id = $%d", *filter.BatchID)
	}
	if filter.From != nil {
		addCond("created_at >= $%d", *filter.From)
	}
	if filter.To != nil {
		addCond("created_at <= $%d", *filter.To)
	}

	whereClause := ""
	if len(conds) > 0 {
		whereClause = " WHERE " + strings.Join(conds, " AND ")
	}

	var total int64
	if err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM notifications"+whereClause, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count notifications: %w", err)
	}

	offset := (page - 1) * pageSize
	args = append(args, pageSize, offset)
	query := fmt.Sprintf(
		"SELECT %s FROM notifications%s ORDER BY created_at DESC, id DESC LIMIT $%d OFFSET $%d",
		notificationSelectColumns, whereClause, len(args)-1, len(args),
	)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Notification, 0, pageSize)
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		items = append(items, *n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	return &domain.NotificationPage{
		Items:      items,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}, nil
}

func (r *NotificationRepo) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'cancelled', cancelled_at = NOW(), next_retry_at = NULL, updated_at = NOW()
		WHERE id = $1 AND status IN ('scheduled', 'pending', 'queued', 'retrying')
		RETURNING `+notificationSelectColumns, id)

	n, err := scanNotification(row)
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("cancel notification: %w", err)
	}

	var exists bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM notifications WHERE id = $1)`, id).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.ErrNotFound
	}
	return nil, domain.ErrNotCancellable
}

func (r *NotificationRepo) Replay(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin replay tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'pending', attempt_count = 0, last_error = NULL, next_retry_at = NULL,
		    processing_started_at = NULL, updated_at = NOW()
		WHERE id = $1 AND status = 'failed'
		RETURNING `+notificationSelectColumns, id)

	n, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM notifications WHERE id = $1)`, id).Scan(&exists); err != nil {
				return nil, err
			}
			if !exists {
				return nil, domain.ErrNotFound
			}
			return nil, domain.ErrNotReplayable
		}
		return nil, fmt.Errorf("replay notification: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO notification_outbox (notification_id, channel, priority, correlation_id, trace_context)
		VALUES ($1, $2, $3, $4, $5)
	`, n.ID, string(n.Channel), string(n.Priority), n.CorrelationID, marshalTraceContext(n.TraceContext)); err != nil {
		return nil, fmt.Errorf("insert replay outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit replay tx: %w", err)
	}
	return n, nil
}

// ClaimForProcessing atomically transitions a notification from queued/retrying to processing.
// Returns (nil, nil) when the notification was already cancelled, delivered, or claimed by another worker.
func (r *NotificationRepo) ClaimForProcessing(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'processing',
		    processing_started_at = NOW(),
		    attempt_count = attempt_count + 1,
		    updated_at = NOW()
		WHERE id = $1 AND status IN ('queued', 'retrying')
		RETURNING `+notificationSelectColumns, id)

	n, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim notification: %w", err)
	}
	return n, nil
}

func (r *NotificationRepo) MarkDelivered(ctx context.Context, id uuid.UUID, providerMessageID string) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'delivered',
		    provider_message_id = $2,
		    delivered_at = NOW(),
		    last_error = NULL,
		    next_retry_at = NULL,
		    processing_started_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
		RETURNING `+notificationSelectColumns, id, providerMessageID)
	n, err := scanNotification(row)
	if err != nil {
		return nil, fmt.Errorf("mark delivered: %w", err)
	}
	return n, nil
}

func (r *NotificationRepo) MarkRetrying(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, lastErr string) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'retrying',
		    next_retry_at = $2,
		    last_error = $3,
		    processing_started_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
		RETURNING `+notificationSelectColumns, id, nextRetryAt, lastErr)
	n, err := scanNotification(row)
	if err != nil {
		return nil, fmt.Errorf("mark retrying: %w", err)
	}
	return n, nil
}

// MarkDeferred returns a notification to retrying without burning an attempt (e.g. when circuit breaker is open).
func (r *NotificationRepo) MarkDeferred(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, lastErr string) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'retrying',
		    attempt_count = GREATEST(attempt_count - 1, 0),
		    next_retry_at = $2,
		    last_error = $3,
		    processing_started_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
		RETURNING `+notificationSelectColumns, id, nextRetryAt, lastErr)
	n, err := scanNotification(row)
	if err != nil {
		return nil, fmt.Errorf("mark deferred: %w", err)
	}
	return n, nil
}

func (r *NotificationRepo) MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) (*domain.Notification, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'failed',
		    last_error = $2,
		    next_retry_at = NULL,
		    processing_started_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
		RETURNING `+notificationSelectColumns, id, lastErr)
	n, err := scanNotification(row)
	if err != nil {
		return nil, fmt.Errorf("mark failed: %w", err)
	}
	return n, nil
}

func (r *NotificationRepo) PromoteScheduled(ctx context.Context, limit int) ([]domain.Notification, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin promote scheduled tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT `+notificationSelectColumns+`
		FROM notifications
		WHERE status = 'scheduled' AND scheduled_at <= NOW()
		ORDER BY scheduled_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query scheduled notifications: %w", err)
	}

	var due []domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan scheduled notification: %w", err)
		}
		due = append(due, *n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(due) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, 0, len(due))
	pgBatch := &pgx.Batch{}
	for i := range due {
		ids = append(ids, due[i].ID)
		due[i].Status = domain.StatusPending
		pgBatch.Queue(`
			INSERT INTO notification_outbox (notification_id, channel, priority, correlation_id, trace_context)
			VALUES ($1, $2, $3, $4, $5)
		`, due[i].ID, string(due[i].Channel), string(due[i].Priority), due[i].CorrelationID, marshalTraceContext(due[i].TraceContext))
	}

	if _, err := tx.Exec(ctx, `
		UPDATE notifications SET status = 'pending', updated_at = NOW()
		WHERE id = ANY($1)
	`, ids); err != nil {
		return nil, fmt.Errorf("update promoted notifications: %w", err)
	}

	br := tx.SendBatch(ctx, pgBatch)
	for i := 0; i < pgBatch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return nil, fmt.Errorf("insert promoted outbox %d: %w", i, err)
		}
	}
	if err := br.Close(); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit promote scheduled tx: %w", err)
	}
	return due, nil
}

func (r *NotificationRepo) ReclaimStaleProcessing(ctx context.Context, leaseDuration time.Duration, limit int) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin reclaim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cutoff := time.Now().Add(-leaseDuration)
	rows, err := tx.Query(ctx, `
		SELECT `+notificationSelectColumns+`
		FROM notifications
		WHERE status = 'processing' AND processing_started_at < $1
		ORDER BY processing_started_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("query stale processing: %w", err)
	}

	var stale []domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		stale = append(stale, *n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}

	for _, n := range stale {
		if n.AttemptCount < n.MaxAttempts {
			if _, err := tx.Exec(ctx, `
				UPDATE notifications
				SET status = 'pending', processing_started_at = NULL, last_error = 'worker lease expired; reclaimed', updated_at = NOW()
				WHERE id = $1
			`, n.ID); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO notification_outbox (notification_id, channel, priority, correlation_id, trace_context)
				VALUES ($1, $2, $3, $4, $5)
			`, n.ID, string(n.Channel), string(n.Priority), n.CorrelationID, marshalTraceContext(n.TraceContext)); err != nil {
				return 0, err
			}
		} else {
			if _, err := tx.Exec(ctx, `
				UPDATE notifications
				SET status = 'failed', processing_started_at = NULL, last_error = 'worker lease expired; max retries exhausted', updated_at = NOW()
				WHERE id = $1
			`, n.ID); err != nil {
				return 0, err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit reclaim tx: %w", err)
	}
	return int64(len(stale)), nil
}

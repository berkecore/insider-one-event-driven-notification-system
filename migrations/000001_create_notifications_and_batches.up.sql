CREATE TABLE IF NOT EXISTS notification_batches (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    request_hash    TEXT NOT NULL DEFAULT '',
    total_count     INTEGER NOT NULL CHECK (total_count > 0 AND total_count <= 1000),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications (
    id                    UUID PRIMARY KEY,
    batch_id              UUID REFERENCES notification_batches(id) ON DELETE SET NULL,
    idempotency_key       TEXT UNIQUE,
    request_hash          TEXT NOT NULL DEFAULT '',
    recipient             TEXT NOT NULL,
    channel               TEXT NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    content               TEXT NOT NULL,
    priority              TEXT NOT NULL CHECK (priority IN ('high', 'normal', 'low')),
    status                TEXT NOT NULL CHECK (status IN ('scheduled', 'pending', 'queued', 'processing', 'retrying', 'delivered', 'failed', 'cancelled')),
    template_id           UUID,
    correlation_id        TEXT NOT NULL DEFAULT '',
    trace_context         JSONB,
    scheduled_at          TIMESTAMPTZ,
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL DEFAULT 5,
    next_retry_at         TIMESTAMPTZ,
    last_error            TEXT,
    provider_message_id   TEXT,
    processing_started_at TIMESTAMPTZ,
    delivered_at          TIMESTAMPTZ,
    cancelled_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notifications_batch_id
    ON notifications (batch_id) WHERE batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_notifications_filter
    ON notifications (status, channel, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_created_at
    ON notifications (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_scheduled_due
    ON notifications (scheduled_at ASC) WHERE status = 'scheduled';

CREATE INDEX IF NOT EXISTS idx_notifications_stale_processing
    ON notifications (processing_started_at ASC) WHERE status = 'processing';

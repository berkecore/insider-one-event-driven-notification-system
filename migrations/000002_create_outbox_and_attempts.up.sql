CREATE TABLE IF NOT EXISTS notification_outbox (
    id              BIGSERIAL PRIMARY KEY,
    notification_id UUID NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    channel         TEXT NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    priority        TEXT NOT NULL CHECK (priority IN ('high', 'normal', 'low')),
    correlation_id  TEXT NOT NULL DEFAULT '',
    trace_context   JSONB,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notification_outbox_unpublished
    ON notification_outbox (id ASC) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS notification_attempts (
    id                  BIGSERIAL PRIMARY KEY,
    notification_id     UUID NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    attempt_number      INTEGER NOT NULL,
    status_code         INTEGER NOT NULL DEFAULT 0,
    provider_message_id TEXT,
    error_message       TEXT,
    retryable           BOOLEAN NOT NULL DEFAULT FALSE,
    duration_ms         BIGINT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notification_attempts_notification_id
    ON notification_attempts (notification_id, attempt_number ASC);

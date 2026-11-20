CREATE TABLE IF NOT EXISTS templates (
    id         UUID PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    channel    TEXT NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    body       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_templates_channel ON templates (channel);

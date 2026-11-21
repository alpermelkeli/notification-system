-- Message templates with {{variable}} placeholders (B-3, ADR-0010). Notifications created from a
-- template store the rendered content, plus the template id and version that produced it for
-- audit. There is no foreign key: a deleted template leaves its notifications untouched, and
-- notifications are retained by partition drop.

-- +goose Up

CREATE TABLE templates (
    id         uuid        PRIMARY KEY, -- UUIDv7
    name       text        NOT NULL UNIQUE,
    channel    text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    body       text        NOT NULL,
    version    integer     NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Nullable columns without a default only change the catalog; no partition is rewritten. A CHECK
-- tying the two together would scan every partition under an exclusive lock, so the storage layer
-- always writes them together instead.
ALTER TABLE notifications
    ADD COLUMN template_id      uuid,
    ADD COLUMN template_version integer;

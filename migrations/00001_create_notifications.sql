-- Notifications, their status history, batches and idempotency keys (ADR-0003, ADR-0004,
-- ADR-0005, ADR-0008). Migrations are forward-only, so there is no Down section.
--
-- Daily partitions of notifications and notification_status_history are created and dropped
-- at runtime (internal/storage/maintenance.go): by `notifier migrate`, then hourly by the
-- dispatcher. The partitioned parents below carry no storage parameters, because PostgreSQL
-- does not allow them on partitioned tables; fillfactor is set on each partition instead.

-- +goose Up

CREATE TABLE batches (
    id             uuid        PRIMARY KEY,
    created_at     timestamptz NOT NULL,
    total          integer     NOT NULL CHECK (total BETWEEN 0 AND 1000), -- items deduplicated by their own key are not counted
    correlation_id text        NOT NULL DEFAULT ''
);

-- Retention: batches older than the notification retention are deleted with them.
CREATE INDEX batches_created_at_idx ON batches (created_at);

CREATE TABLE notifications (
    -- UUIDv7; created_at is the millisecond timestamp embedded in id, so a lookup by id alone
    -- can be pruned to one partition. Unique indexes on a partitioned table must contain the
    -- partition key, hence the composite primary key.
    id                  uuid        NOT NULL,
    created_at          timestamptz NOT NULL,
    -- No foreign key: batches is not partitioned and rows are retained by partition drop.
    batch_id            uuid,

    channel             text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    priority            text        NOT NULL CHECK (priority IN ('high', 'normal', 'low')),
    recipient           text        NOT NULL,
    content             text        NOT NULL,

    status              text        NOT NULL DEFAULT 'pending' CHECK (status IN (
                            'pending', 'queued', 'processing', 'retrying',
                            'sent', 'failed', 'cancelled')),
    attempts            integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    -- Drives initial dispatch, scheduled delivery and retry backoff (ADR-0001).
    next_attempt_at     timestamptz NOT NULL,
    scheduled_at        timestamptz,
    queued_at           timestamptz,
    lease_until         timestamptz,
    -- Fencing token of the current claim; outcome transitions must present it (ADR-0004).
    lease_token         uuid,
    sent_at             timestamptz,
    provider_message_id text,
    last_error          text,

    correlation_id      text        NOT NULL DEFAULT '',
    updated_at          timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (id, created_at),
    CHECK ((status = 'processing') = (lease_until IS NOT NULL AND lease_token IS NOT NULL)),
    CHECK (status <> 'sent' OR sent_at IS NOT NULL)
) PARTITION BY RANGE (created_at);

-- Dispatcher: due work in next_attempt_at order (ADR-0003). Only unpublished rows are in it, so
-- it stays small however large the table grows.
CREATE INDEX notifications_due_idx ON notifications (next_attempt_at)
    WHERE status IN ('pending', 'retrying');

-- Reaper: expired worker leases (ADR-0003). At most a few hundred rows are processing at once.
CREATE INDEX notifications_lease_idx ON notifications (lease_until)
    WHERE status = 'processing';

-- Reaper: queued rows that never reached a worker (ADR-0003). Small in steady state; during a
-- burst it holds the Kafka backlog, which is why the reaper's age threshold is configurable.
CREATE INDEX notifications_queued_idx ON notifications (queued_at)
    WHERE status = 'queued';

-- List with a status filter (FR-1.5). Lists are ordered by id, which is time-ordered.
CREATE INDEX notifications_status_id_idx ON notifications (status, id);

-- Batch status and batch listing (FR-1.3).
CREATE INDEX notifications_batch_idx ON notifications (batch_id, id)
    WHERE batch_id IS NOT NULL;

-- Append-only audit trail of every transition (ADR-0005). It is partitioned by the parent
-- notification's created_at, so a notification and its history are dropped together.
CREATE TABLE notification_status_history (
    id                      bigint      GENERATED ALWAYS AS IDENTITY,
    notification_id         uuid        NOT NULL,
    notification_created_at timestamptz NOT NULL,
    from_status             text,
    to_status               text        NOT NULL,
    attempt                 integer     NOT NULL,
    reason                  text        NOT NULL DEFAULT '',
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, notification_created_at)
) PARTITION BY RANGE (notification_created_at);

CREATE INDEX notification_status_history_notification_idx
    ON notification_status_history (notification_id, id);

-- Idempotency-Key → created resource (ADR-0004). Not partitioned, because the key alone must be
-- unique. Expired keys are deleted by the maintenance loop.
CREATE TABLE idempotency_keys (
    key                 text        PRIMARY KEY CHECK (length(key) BETWEEN 1 AND 255),
    -- SHA-256 of the canonicalised request.
    request_hash        bytea       NOT NULL CHECK (length(request_hash) = 32),
    resource_type       text        NOT NULL CHECK (resource_type IN ('notification', 'batch')),
    resource_id         uuid        NOT NULL,
    resource_created_at timestamptz NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    expires_at          timestamptz NOT NULL
);

CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

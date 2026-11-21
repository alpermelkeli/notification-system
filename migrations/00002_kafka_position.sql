-- Where the dispatcher published each queued notification, so the reaper can tell a record that
-- workers consumed without claiming (lost) from one still waiting in the backlog (ADR-0003
-- amendment). The topic follows from channel and priority, so only partition and offset are
-- stored. Both are NULL until the row is first queued and are cleared when it is requeued.

-- +goose Up

-- Adding nullable columns without a default only changes the catalog; no partition is rewritten.
ALTER TABLE notifications
    ADD COLUMN kafka_partition integer,
    ADD COLUMN kafka_offset    bigint;

-- Reaper: for each work partition, the queued rows behind the consumer group's committed offset.
-- Partial, so it holds only the Kafka backlog. Creating it on the partitioned parent builds it on
-- every daily partition while blocking writes; that is fine for a fresh schema. On a large live
-- table, create it ON ONLY the parent, build each partition's index CONCURRENTLY and attach it.
CREATE INDEX notifications_queued_position_idx
    ON notifications (channel, priority, kafka_partition, kafka_offset)
    WHERE status = 'queued';

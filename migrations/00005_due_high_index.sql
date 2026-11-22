-- The dispatcher takes due `high` rows before the rest (ADR-0003, Phase 9 amendment). This index
-- holds only due `high` rows, so finding them stays cheap however many `normal` and `low` rows
-- are waiting in notifications_due_idx after a burst.

-- +goose Up

-- Creating it on the partitioned parent builds it on every daily partition while blocking writes;
-- that is fine for a fresh schema. On a large live table, create it ON ONLY the parent, build each
-- partition's index CONCURRENTLY and attach it.
CREATE INDEX notifications_due_high_idx ON notifications (next_attempt_at)
    WHERE status IN ('pending', 'retrying') AND priority = 'high';

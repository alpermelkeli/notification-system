# 0003. Dispatch via the notifications table (no separate outbox table)

- **Status:** accepted
- **Date:** 2026-11-21

## Context
Writing to PostgreSQL and publishing to Kafka cannot be done atomically (the dual-write
problem):
- If the DB commit succeeds and the publish fails, the notification is stuck forever.
- If the publish succeeds and the DB commit fails, a worker receives an id that does not exist.

The usual answer is a transactional outbox. We also need a component that re-publishes
notifications when a schedule (B-2) or retry backoff (FR-3.1) becomes due.

## Options considered
1. **Separate outbox table + relay.** Standard, but it duplicates data we already store in
   `notifications` and doubles write volume.
2. **API publishes directly after commit, with a sweeper for stragglers.** Lower latency, but
   duplicate publishes become routine rather than exceptional, and we still need the sweeper.
3. **The notifications table is the outbox (chosen).** The API only writes the row
   (`status=pending`, `next_attempt_at=now()` or the scheduled time) and never talks to Kafka.
   The dispatcher publishes due rows.
4. **CDC (Debezium).** Robust, but it adds Kafka Connect and is too heavy for this scope.

## Decision
**Dispatcher loop** (any number of instances can run concurrently):
```sql
BEGIN;
SELECT id, channel, priority, attempts, next_attempt_at, correlation_id FROM notifications
 WHERE status IN ('pending','retrying') AND next_attempt_at <= now()
 ORDER BY next_attempt_at          -- FIFO: priority is enforced only by the worker scheduler
 LIMIT 500
 FOR UPDATE SKIP LOCKED;
-- publish batch to Kafka (idempotent producer, acks=all) and wait for acks
UPDATE notifications SET status='queued', queued_at=now() WHERE id = ANY($ids);
COMMIT;            -- on publish failure: ROLLBACK, back off, retry
```
- **No priority ordering here.** Publishing is not rate-limited and runs far above 300 msg/s,
  so FIFO by `next_attempt_at` keeps every lane flowing into Kafka. Ordering by priority here
  could starve `low` at the publish stage and would duplicate the worker scheduler's job
  (ADR-0006).
- **Wake-up:** the API issues `NOTIFY notifications_due` after inserting rows that are due now,
  and the dispatcher `LISTEN`s. A polling fallback (default 500 ms) catches scheduled and retry
  rows.
- **Reaper** (part of the dispatcher, runs every 30 s):
  - `processing` rows whose `lease_until` has expired go to `retrying`. This covers workers
    that crashed mid-send.
  - `queued` rows older than the stuck threshold (default 10 min) go back to `pending` to be
    re-published. This covers records skipped by an offset commit after a consumer crash.
- **Status events** (`notification.events`) are published **best-effort after commit** by the
  component that changed the state. PostgreSQL remains the truth. Losing an event only delays a
  WebSocket update, and clients can always query the API.

## Consequences
- ✅ No dual write: the row *is* the publish intent, so it cannot be lost.
- ✅ One mechanism handles new, scheduled and retrying notifications.
- ⚠️ At-least-once publishing: a crash between the Kafka ack and `COMMIT` re-publishes the batch.
  Workers deduplicate with a guarded claim (ADR-0004).
- ⚠️ Status events are not guaranteed. If a consumer that cannot tolerate loss is added (e.g.
  billing), introduce an outbox or CDC for events.

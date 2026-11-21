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
SELECT id, created_at, channel, priority, attempts, next_attempt_at, correlation_id FROM notifications
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
  - `queued` rows whose Kafka record the workers have already **consumed past** go back to
    `pending` to be re-published (see the amendment below; this replaces the original fixed
    10-minute threshold). This is the safety net for records that never reach a worker. Normal
    crash recovery is handled by watermark offset commits (ADR-0002).
- **Status events** (`notification.events`) are published **best-effort after commit** by the
  component that changed the state. PostgreSQL remains the truth. Losing an event only delays a
  WebSocket update, and clients can always query the API.

**Amendment (2026-11-21, before Phase 5): the `queued` reaper compares Kafka offsets, not ages.**

*Problem with the original rule.* "Back to `pending` after 10 minutes in `queued`" cannot tell a
lost record from one that is simply waiting. During a burst, `queued` rows legitimately wait in
Kafka for hours (2 M SMS ≈ 5.6 h at 100 msg/s), so the rule would re-publish the whole backlog.
It also could not be tuned: any fixed age is either too short for a large burst or too long for
real loss.

*Decision.*
1. When the dispatcher marks a published batch `queued`, it also stores where each record
   landed: `kafka_partition` and `kafka_offset` (from the produce acks it already waits for, in
   the same `UPDATE`). The topic follows from `channel` and `priority`. This needs migration
   00002 (two nullable columns).
2. The reaper reads the committed offset of each work partition for the channel's consumer group
   (`workers.{channel}`) through the Kafka admin API.
3. A `queued` row is **lost** if its `kafka_offset` is below the committed offset of its
   partition: with watermark commits (ADR-0002), the group only commits past a record after
   handling it, so a record behind the committed offset was seen and dropped without a claim. A
   short grace period (default 1 minute since `queued_at`) avoids racing a commit in flight. Lost
   rows go back to `pending` with reason `kafka record consumed without claim`.
4. A row whose offset is at or above the committed offset is still waiting in the backlog and is
   never touched, however long the backlog is.
5. Fallback: a much larger age threshold (default 24 h, configurable) catches cases offsets
   cannot, such as a topic recreated with offsets starting from zero again, or a row without
   stored offsets.

*Why this matters beyond data loss.* It also closes a race in the normal path: the dispatcher
commits `queued` only after Kafka acknowledges, so a fast worker can receive a record while its
row is still `pending`. The guarded claim (`WHERE status = 'queued'`) then skips it, and the
worker commits past it. The row becomes `queued` with an offset below the committed one, so the
reaper re-publishes it after the grace period. (Phase 5 may additionally let the worker retry a
claim briefly when the row is still `pending`, which makes this path rare.)

*Consequences.* Burst backlogs are never re-published. Real loss is detected within about a
reaper interval plus the grace period, not after an arbitrary age. The cost is two columns and one
admin request per channel per reaper run (a handful of partitions), and the reaper needs access
to the Kafka admin API.

**Amendment (2026-11-21, Phase 5): implementation notes.**
- Committed offsets are read with franz-go's admin client (`kadm.FetchOffsets` on
  `workers.{channel}`), from the same client the dispatcher produces with.
- Every dispatcher replica runs the reaper, **without an advisory lock**. Each step is one
  statement that locks its rows with `FOR UPDATE SKIP LOCKED`, so two reapers never change the
  same row, and the offset reads are idempotent. A lock would only save a few small requests per
  30 s, at the cost of a dedicated connection. Partition maintenance, by contrast, needs its lock
  for correctness (ADR-0008).
- The dispatch race described above is closed in the database instead of with claim retries:
  every guarded transition first locks the row by its key and checks the status on the locked
  version. The dispatcher holds the row lock from before publishing until it commits `queued`, so
  a worker that receives the record early waits for that commit and then claims normally. (With
  the status in the locking query's `WHERE`, PostgreSQL rejected the still-`pending` row from the
  statement snapshot without waiting; a compose burst test lost 1 of 100 records this way, and the
  reaper recovered it.)
- Each reaper step changes at most 1000 rows per statement and 10 statements per run; the next run
  continues after a mass failure.

## Consequences
- ✅ No dual write: the row *is* the publish intent, so it cannot be lost.
- ✅ One mechanism handles new, scheduled and retrying notifications.
- ⚠️ At-least-once publishing: a crash between the Kafka ack and `COMMIT` re-publishes the batch.
  Workers deduplicate with a guarded claim (ADR-0004).
- ⚠️ Status events are not guaranteed. If a consumer that cannot tolerate loss is added (e.g.
  billing), introduce an outbox or CDC for events.

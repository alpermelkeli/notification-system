# 0005. Delivery, retry and failure handling

- **Status:** accepted
- **Date:** 2026-11-21

## Context
The brief leaves delivery and retry logic to the candidate (FR-3). Requirements: retry
intelligently, never hammer a failing provider, never lose a notification silently, and make
terminal failures visible and replayable (B-1).

## Decision

### Status lifecycle (FR-3.4)
```
            ┌──────────── cancel ─────────────┐
            │                                 ▼
 pending ──► queued ──► processing ──► sent   cancelled
   ▲  (dispatcher)  (worker claim)  │
   │                                ├──► retrying ──(next_attempt_at due)──► queued
   │                                └──► failed ──(replay)──► pending
   └── scheduled rows are `pending` with a future next_attempt_at
```
- Cancellable states: `pending`, `queued`, `retrying` (FR-1.4). A `queued` message that is
  already in Kafka is dropped by the worker's guarded claim (ADR-0004).
- Terminal states: `sent`, `failed`, `cancelled`. Every transition appends a row to
  `notification_status_history` (from, to, reason, attempt, timestamp).

### Failure classification (FR-3.2)
| Provider outcome | Class | Action |
|------------------|-------|--------|
| `2xx` (expected `202` + `messageId`) | success | `sent`, store `provider_message_id` |
| timeout, connection error, `5xx`, `408`, `425` | retryable | backoff |
| `429` | retryable (backpressure) | `next_attempt_at = max(backoff, Retry-After)`, fleet-wide cool-down (ADR-0006), not counted by the circuit breaker |
| other `4xx` | permanent | `failed` immediately |
| circuit open | not attempted | release to `retrying` without consuming an attempt |

### Retry policy (FR-3.1)
- Exponential backoff with equal jitter:
  `d = min(base · 2^(attempt-1), max)`, `delay = d/2 + rand(0, d/2)`.
- Defaults: `base=5s`, `max=5m`, `max_attempts=5`, so delays are about 5 s → 10 s → 20 s → 40 s,
  then `failed`. All values are configurable via env.
- Retries are **out-of-band:** the worker writes `retrying` with `next_attempt_at` and commits
  the Kafka offset. The dispatcher re-publishes when the row is due (ADR-0003). Workers never
  sleep on a message.
- A retry keeps its original priority, and `attempt` never influences scheduling (ADR-0006).

### Dead letters (FR-3.3, B-1)
- When attempts are exhausted or a failure is permanent:
  - the status becomes `failed` and `last_error` is stored,
  - the message is published to `notifications.dlq` (best effort, for audit and external
    tooling).
- `GET /v1/notifications?status=failed` lists dead letters.
  `POST /v1/notifications/{id}/retry` replays one: it resets the row to `pending` with a fresh
  attempt budget.

### Circuit breaker (B-1)
- One breaker per channel per worker instance (`sony/gobreaker`).
- Opens after 5 consecutive failures, or a >50% failure ratio over at least 20 requests.
  Stays open for 30 s, then half-opens with a single probe.
- While open, the worker **pauses fetching** that channel's topics (`PauseFetchTopics`) and
  releases already-claimed records back to `retrying`. Messages wait safely in Kafka and no
  attempts are burned.

### Provider client
- HTTP client with a 10 s timeout and connection pooling. The provider URL comes from config
  (`PROVIDER_URL`), never hard-coded.
- Request body: `{to, channel, content}`, plus `Idempotency-Key` and `X-Correlation-ID`
  headers.

**Amendment (2026-11-21, Phase 6): implementation notes.**
- *Classification* lives in `provider.Classify`. `last_error` starts with the class
  (`retryable:`, `rate_limited:`, `permanent:`), or `attempts exhausted (n), last <class>:` when
  the budget is spent, so `GET /v1/notifications?status=failed` shows why each row failed.
- *Backoff* is `internal/retry` with the formula above. A `429` waits
  `max(backoff, min(Retry-After, 1m))`; the cap stops a bogus header from parking a row for days.
  **Change:** a `429` releases the notification (`Release`) instead of consuming the attempt. A
  provider whose real limit is below ours would otherwise fail healthy notifications after five
  throttled answers. Throttled notifications wait instead, up to retention, like during an outage.
  `RETRY_DELAY` became `RETRY_BASE_DELAY` and `RETRY_MAX_DELAY`. `MAX_ATTEMPTS` is shared by the
  worker and the dispatcher's reaper.
- *Expired lease on the last attempt.* The reaper keeps an expired lease's attempt counted (the
  send may have happened). A row that expires on attempt `MAX_ATTEMPTS` now goes to `failed` with
  a dead letter in the same statement, instead of being dispatched beyond its budget.
- *Dead letters* are claim-checks: ids, channel, priority, attempts, `last_error`, correlation id
  and time, but no recipient or content, so the topic holds no personal data. They are published
  after the `failed` row commits, by the worker or by the reaper. A crash in between loses only
  the audit record, never the notification.
- *Circuit breaker.* `sony/gobreaker/v2` in two-step mode. Retryable failures count as failures,
  `2xx` and permanent `4xx` as successes (the provider answered), and `429`s are excluded. The
  ratio rule uses a rolling one-minute window with six buckets. Two changes from the text above:
  - The breaker is checked **before** the pick, not after the claim. While it is open nothing is
    picked, so records stay in the worker's priority buffers and fetching pauses through the
    buffers' high-water mark, the same backpressure that pauses a busy topic. There is no separate
    `PauseFetchTopics` call. The watermark does not move, so the backlog waits in Kafka and no
    attempt is burned or released.
  - A send admitted while the breaker was closed, but claimed after it opened, is released to
    `retrying` without consuming its attempt, due after the open period. Sends already at the
    provider when it trips finish normally.
  - In half-open, exactly one probe is admitted. An admitted send that does not happen (stale
    record, no token) is reported as excluded, so the next probe is not blocked.
- *Lease budget.* Deadlines run from the start of the claim, which is no later than the
  database's `now()` that starts the lease:
  - the provider call must end 5 s before the lease does, so its outcome is still fenced by the
    lease;
  - the fresh token after a slow claim is waited for at most 2 s, so a long cool-down hands the
    attempt back instead of eating the lease;
  - outcome writes are retried while the database is unavailable until the lease ends, each try
    with its own 2 s deadline. A delivered message therefore survives a database outage shorter
    than the lease without being reaped and sent again. Measured in compose before this change:
    10 of 1000 were sent twice during a 15 s outage.
  - A write retried after an ambiguous failure that the lease guard then rejects is checked
    against the row: if the row already holds the outcome, it counts as written, so the dead
    letter of a `failed` row is not skipped.
- *Idempotent claim.* The worker generates the lease token once per record. A claim retried after
  a lost answer gets its own claim back instead of skipping the record as stale. Without that, the
  reaper would later count the attempt, and on the last attempt fail a notification that was never
  sent.
- *Half-open.* A send admitted while the breaker was closed is released if, after its claim, the
  breaker is no longer closed. Only the probe reaches a recovering provider.
- *PostgreSQL outage (from the Phase 5 review).* A claim that fails with a transient database
  error (`storage.IsTransient`: no connection, timeouts, shutdown, resource exhaustion,
  serialization failures) is retried with backoff from 100 ms up to 5 s instead of being given up.
  It holds its sender slot, so a worker whose database is down stops taking work, and its
  watermark holds. On shutdown or revoke the claim is abandoned below the watermark for the next
  owner. A non-transient error still skips the record, leaving it to the reaper.

## Consequences
- ✅ Transient provider issues heal automatically. Permanent errors fail fast. Outages pause
  delivery instead of draining the retry budget.
- ✅ Every failure is visible (status, `last_error`, history, metrics, DLQ topic) and replayable.
- ⚠️ Backoff granularity is bounded by the dispatcher poll interval (500 ms), which is fine at
  second-level delays.
- ⚠️ Breaker state is per instance, not global. This is acceptable because each instance
  protects itself, and a shared breaker can be added later via Redis.

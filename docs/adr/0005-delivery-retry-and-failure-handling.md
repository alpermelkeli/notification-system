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

## Consequences
- ✅ Transient provider issues heal automatically. Permanent errors fail fast. Outages pause
  delivery instead of draining the retry budget.
- ✅ Every failure is visible (status, `last_error`, history, metrics, DLQ topic) and replayable.
- ⚠️ Backoff granularity is bounded by the dispatcher poll interval (500 ms), which is fine at
  second-level delays.
- ⚠️ Breaker state is per instance, not global. This is acceptable because each instance
  protects itself, and a shared breaker can be added later via Redis.

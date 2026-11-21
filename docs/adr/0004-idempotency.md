# 0004. Idempotency: no duplicate creates, no duplicate sends

- **Status:** accepted
- **Date:** 2026-11-21

## Context
FR-2.5 requires idempotency to prevent duplicate sends. Duplicates can enter at every hop:
- client retries of the create request,
- dispatcher re-publishes (ADR-0003),
- Kafka redelivery after a rebalance or crash,
- a worker crash after the provider accepted a message.

Exactly-once delivery to an external HTTP provider is impossible, so the goal is to make every
hop idempotent and to make the remaining window explicit.

## Decision
**1. API (create / batch)**
- Optional `Idempotency-Key` header on `POST /v1/notifications` and
  `POST /v1/notifications/batch`. Batch items may additionally carry their own `idempotencyKey`.
- The key is stored with a SHA-256 hash of the canonicalised request (`request_hash`) in a
  dedicated, non-partitioned `idempotency_keys` table (key as primary key → notification or
  batch id). It is kept separate because `notifications` is partitioned by `created_at`
  (ADR-0008), and a partitioned table cannot enforce a unique index that excludes the
  partition key. The key row and the notification rows are inserted in the same transaction. The insert uses `ON CONFLICT DO NOTHING`, then reads back:
  - same key and same hash → return the original resource (`200` / `202` with the same body),
  - same key and different hash → `409 idempotency_key_reused`.
- Concurrent requests with the same key resolve through the unique index; no locks are needed.

**2. Worker claim (guarded transition)**
```sql
UPDATE notifications
   SET status='processing', attempts=attempts+1, lease_until=now()+interval '60 seconds'
 WHERE id=$1 AND status='queued'
RETURNING recipient, channel, content, attempts, ...;
```
Zero rows means the message is cancelled, already claimed, or already finished. It is skipped
and acked. This single guard removes duplicates from the dispatcher and from Kafka.

**3. Outcome transitions** are guarded on `status='processing'`, so a late worker whose lease
was reaped cannot overwrite a newer outcome.

**4. Provider:** every request carries `Idempotency-Key: <notification id>` and
`X-Correlation-ID`, so providers that support deduplication can deduplicate.

**Amendment (2026-11-21, Phase 3), details settled during implementation:**
- An expired key that the cleanup has not deleted yet is taken over by the next request
  (`ON CONFLICT ... DO UPDATE ... WHERE expires_at <= now()`).
- Batch item keys: an item whose key is live with the same request hash is not created again; the
  response returns the existing notification for that position, marked as a duplicate. Such
  items do not belong to the new batch and are not counted in its total. A different hash fails
  the whole batch with `409`. Duplicate keys inside one request are rejected as invalid.
- Replaying a batch with its batch-level key returns the original batch and the notifications it
  created, in request order.
- The key, the batch row and all notification rows are written in one transaction. Keys are
  claimed in sorted order so that concurrent batches with overlapping keys cannot deadlock.
- **Fencing token (replaces the status-only guard of point 3):** a status guard alone is not
  enough. If worker A stalls past its lease, the row is reaped, re-dispatched and claimed by
  worker B, so it is `processing` again and A's late outcome would match. Each claim therefore
  sets a fresh `lease_token`, and every outcome transition (`sent`, `retrying`, `failed`,
  release) must present it; a stale token is rejected as "lease lost".
- `IDEMPOTENCY_TTL` must not exceed `RETENTION_DAYS`, so a live key never points at a dropped
  notification.

## Consequences
- ✅ Duplicate creates are impossible, and duplicate publishes or deliveries are filtered before
  the provider call.
- ⚠️ **Remaining window (documented as at-least-once):** if a worker crashes after the provider
  accepted the request but before persisting `sent`, the reaper retries it and the provider may
  receive it twice. This is mitigated by the provider idempotency header.
- ⚠️ Idempotency keys expire after a TTL (default 72 h, removed by a periodic cleanup). A
  client retry after the TTL creates a new notification, which is the standard trade-off. In
  production, also scope keys per API client or tenant.

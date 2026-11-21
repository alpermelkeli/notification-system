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
- The key is stored with a SHA-256 hash of the canonicalised request (`request_hash`) and
  protected by a unique index. The insert uses `ON CONFLICT DO NOTHING`, then reads back:
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

## Consequences
- ✅ Duplicate creates are impossible, and duplicate publishes or deliveries are filtered before
  the provider call.
- ⚠️ **Remaining window (documented as at-least-once):** if a worker crashes after the provider
  accepted the request but before persisting `sent`, the reaper retries it and the provider may
  receive it twice. This is mitigated by the provider idempotency header.
- ⚠️ Idempotency keys are kept indefinitely in this scope. In production, add a TTL cleanup
  (e.g. 24–72 h) and scope keys per API client or tenant.

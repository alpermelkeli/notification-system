# 0006. Rate limiting and priority scheduling

- **Status:** accepted
- **Date:** 2026-11-21

## Context
- FR-2.2: at most **100 messages per second per channel**. This must hold across all worker
  instances.
- FR-2.3: high / normal / low priority.

Because the rate limit is the bottleneck during bursts, **priority only matters at the moment
a send slot becomes available**. Kafka has no native priority (ADR-0002).

## Options considered
- **Rate limiter**
  1. In-process limiter (`x/time/rate`) set to 100 / N instances. This breaks whenever N changes
     and requires coordination.
  2. Partition-based quota. Complex and coupled to rebalances.
  3. **Distributed GCRA in Redis (chosen)** via `go-redis/redis_rate`: one atomic Lua script per
     check and a single key per channel. It is exact across instances.
- **Priority**
  1. Strict priority everywhere. Simple, but low priority can starve under sustained load.
  2. Strict high, weighted round-robin normal/low (e.g. 4:1). Prevents starvation, but
     ignores how long a message has actually waited.
  3. Uncapped aging (`base + wait bonus`). Fair, but a long-waiting `low` eventually overtakes a
     fresh `high` (OTP), which is unacceptable.
  4. **Capped aging + minimum non-high share (chosen).**

## Decision
**Rate limiting**
- The key is `ratelimit:{channel}:{provider}`, with a limit of 100/s and a small burst (default 10,
  changed to 1 in Phase 5, see the amendment). Configurable via env.
- A worker acquires a token **before** each provider call. When denied, it waits for the
  `RetryAfter` returned by GCRA. Messages are never dropped or re-queued because of the limiter.
- If Redis is unavailable, the worker **fails closed**: it pauses sending and reports not-ready.
  Exceeding the provider limit is worse than a short delay.

**Provider limits (the reason the limit exists)**
Real providers (SMS gateways, email APIs, FCM/APNs) enforce their own quotas, and the
100 msg/s requirement models that. The limiter is therefore designed around the *provider*, not
around our own capacity:
- **Limits are configured per channel** (`RATE_LIMIT_SMS`, `RATE_LIMIT_EMAIL`, `RATE_LIMIT_PUSH`,
  default 100/s each), because real providers differ (e.g. SMS ~100/s, email ~600/s). The
  limiter key is the provider endpoint (`ratelimit:{channel}:{provider}`), so if a channel gets
  a second (fallback) provider, it gets its own budget.
- **We stay below the provider limit:** the configured rate should leave headroom (e.g. 90% of
  the contractual limit) for clock skew and other clients sharing the account.
- **Shared cool-down on `429`:** when any worker receives `429` from a provider, it writes
  `ratelimit:{channel}:{provider}:cooldown` in Redis with a TTL equal to `Retry-After`
  (default 1 s, capped at 60 s). Every worker checks this key before acquiring a token, so the
  **whole fleet** backs off, not just the worker that was throttled. The throttled notification
  goes to `retrying` (ADR-0005).
- **`429` is backpressure, not an outage:** it does not count as a circuit-breaker failure.
  Only timeouts, connection errors and `5xx` do.
- **Local development:** webhook.site is the provider in the brief, but it has its own usage
  limits and is unsuitable for load or failure testing. docker-compose therefore also ships a
  small **mock provider** with configurable latency, error rate and 429 rate. `PROVIDER_URL`
  selects between them.

**Separation of concerns**
```
Kafka batch fetch → route by topic → priority heaps → aging scheduler → Redis limiter → provider → DB outcome → watermark commit
   (distribute)                     (bounded, pause)   (prioritise)     (rate limit)
```
- Kafka distributes work. Partition counts (6/6/3) size **throughput and parallelism only**;
  they are not a priority mechanism.
- The application scheduler inside each worker is the **only** place where priority is
  enforced. The dispatcher publishes due rows in FIFO order (ADR-0003).

**Priority scheduling: capped aging (per channel, inside each worker)**
- Fetched batches are routed by topic into one buffer per priority (high-water mark 50
  records, low-water mark 25). When a buffer is full, fetching from that topic pauses, which
  applies backpressure into Kafka (ADR-0002).
- **Each buffer is a min-heap ordered by `due_at`**, not a plain FIFO queue. Records from
  several partitions arrive interleaved, so arrival order is not age order. The heap makes the
  head the truly oldest record. At ~50 elements, push and pop are negligible (O(log n)).
- The scheduler **first waits for a rate-limit token, then picks**. A pick made before waiting
  for a token could hold a low-priority record while a high-priority one arrives.
- The pick compares the **effective priority** of the three buffer heads:
  ```
  effective = base + min(aging_rate × wait_seconds, cap)
  wait_seconds = now − due_at        (due_at = next_attempt_at, carried in the claim-check)
  ```

  | Priority | Base | Cap on bonus | Max effective |
  |----------|------|--------------|---------------|
  | high     | 100  | 0            | 100           |
  | normal   | 50   | 45           | 95            |
  | low      | 10   | 80           | 90            |

  The default aging rate is 2 points/s. A `low` record therefore overtakes a fresh `normal` one
  after ~20 s of waiting. **Nothing ever overtakes `high`:** OTPs and security alerts keep their
  latency. Ties are broken by the older `due_at`.
- **Minimum share for non-high traffic:** under a sustained flood of `high`, every 10th token
  (10%, configurable) goes to the highest-effective non-high head, if one is waiting. This puts
  an upper bound on starvation without letting bulk traffic delay transactional messages
  noticeably.
- **Wait time is measured from `due_at`, not `created_at`.** Otherwise retries and scheduled
  notifications would be treated as having waited since creation and would jump the queue.
- **`attempt` never changes priority.** A retry keeps its original priority. Retry storms are
  handled by the retry mechanism itself (out-of-band backoff with jitter, circuit breaker, fleet
  cool-down on `429`; ADR-0005), not by the scheduler.
- Picked records go to a bounded sender pool (default 20 goroutines per channel), sized for
  provider latency: 100 msg/s × ~200 ms ≈ 20 in flight. When a send completes, its offset is
  marked in the partition watermark tracker (ADR-0002).
- All weights, caps, the aging rate and the minimum share are configurable via env.

**Amendment (2026-11-21, Phase 5): implementation notes.**
- The `{provider}` part of the keys is the host of `PROVIDER_URL`. `redis_rate` adds its own
  `rate:` prefix to the GCRA key.
- The cool-down is set with `SET NX PX` plus `PEXPIRE ... GT`, so a shorter `Retry-After` never
  shortens a cool-down already in place.
- **The default burst is 1, not 10.** After an idle period GCRA grants the burst at once and then
  the steady rate, so one wall-clock second can hold up to `rate + burst - 1` sends. A compose
  burst test measured 106 sends in one second at provider side with burst 10, which breaks
  "max 100 msg/s". With burst 1, sends are spaced 10 ms apart: the same test peaked at 91/s and
  sent 1100 SMS in about 12.8 s. Burst 1 leaves GCRA no slack, so every late token request (timer
  and Redis round-trip latency) is lost throughput; the sustained rate is about 90% of the limit,
  which is the headroom this ADR asks for anyway. `RATE_LIMIT_BURST=2` would reach the full rate
  at the cost of an occasional 101st send within one second. `RATE_LIMIT_BURST` stays
  configurable for providers that tolerate short bursts.
- A pick can find nothing after the token was granted (the partition was revoked meanwhile). That
  token is lost, and so is the token of a record skipped as stale. Both are rare and only cost
  throughput.

## Consequences
- ✅ 100 msg/s per channel holds regardless of the number of worker replicas.
- ✅ A marketing blast in `low` cannot delay OTPs in `high`. `low` catches up with `normal`
  through aging and keeps progressing under a `high` flood through the minimum share.
- ✅ The scheduler is a pure function of (buffer heads, clock), so it is easy to unit-test with
  an injected clock.
- ⚠️ The minimum share means a `high` flood can be delayed by up to 10%. This is deliberate. In
  production, also apply per-client quotas on `high` to stop abuse of the top lane.
- ⚠️ Priority is enforced per worker replica over the partitions it owns, not globally. With
  the range assignor every replica owns partitions of every priority (ADR-0002), so this is
  close to global in practice.
- ⚠️ Redis becomes a hard dependency for sending (not for accepting requests).

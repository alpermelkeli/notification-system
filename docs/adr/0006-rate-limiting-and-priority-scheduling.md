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
  3. **Distributed GCRA in Redis (chosen)**: one atomic Lua script per check and a single key per
     channel. It is exact across instances. (Planned with `go-redis/redis_rate`; Phase 5 replaced
     it with a reserving script, see the amendment.)
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
  `RetryAfter` returned by GCRA (Phase 5: it reserves the next slot and sleeps until it, see the
  amendment). Messages are never dropped or re-queued because of the limiter.
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
  enforced. The dispatcher publishes due rows in FIFO order (ADR-0003). (Phase 9: it takes due
  `high` rows first, so they do not wait behind a burst; see the ADR-0003 amendment.)

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

**Amendment (2026-11-21, Phase 5): a reserving GCRA replaces `redis_rate`, and the burst default is 1.**

*Measurements.* A compose burst (1000 `low` SMS, then 100 `high`) against the mock provider
showed:
- With `redis_rate` and burst 10, one second at the provider held 106 sends. GCRA grants the burst
  at once after an idle period, so a wall-clock second can hold `rate + burst - 1` sends. That
  breaks "max 100 msg/s".
- With `redis_rate` and burst 1, the peak was 91/s with one worker, and 1000 SMS took 14.8 s with
  three workers (about 68/s). `redis_rate` answers "allowed, or retry after X". A rejected request
  leaves its slot to whoever asks first, and the retry latency (timer plus Redis round trip) is
  lost. The more replicas contend, the more slots go unused.

*Decision.* `internal/ratelimit` runs its own small Lua script: a **reserving** GCRA. Each call
atomically takes the next free slot of the channel (slots are `1/rate` apart, plus
`(burst-1)/rate` of tolerance) and returns how long to sleep until it. A reservation is never
lost to contention, so the schedule advances by exactly `1/rate` per send, whatever the number of
workers. The same script checks the cool-down key first, so each token costs one round trip. Slots
are reserved at most 1 s ahead. Beyond that the caller waits without reserving and asks again, so
a waiter that is cancelled holds no distant slot. The burst default is **1**
(`RATE_LIMIT_BURST`).

*Result* (same test; rate measured on arrival at the provider): 1100 SMS in 11.1 s with one
worker and 11.4 s with three (the floor at 100/s is 11.0 s). The peak was 100/s in both runs, and
the 100 `high` were done within 1.4 s of submission despite the `low` backlog.

*Other implementation notes.*
- Keys are `{ratelimit:{channel}:{provider}}` and `...:cooldown`. The braces are a Redis Cluster
  hash tag, so one script can use both keys. `{provider}` is the host of `PROVIDER_URL`.
- The cool-down is set with `SET NX PX` plus `PEXPIRE ... GT`, so a shorter `Retry-After` never
  shortens a cool-down already in place.
- The token is granted before the claim. If the claim took longer than 50 ms (e.g. it waited for
  the dispatcher's commit), the worker takes a fresh token before sending (and, since Phase 9,
  every send waits for the same 50 ms offset after its token; see the amendment below). Otherwise sends whose
  claims finished together would reach the provider in a burst of up to `WORKER_CONCURRENCY` per
  replica above the rate. If no fresh token can be had (Redis down), the claim is released
  without consuming the attempt.
- A token can go unused: when a pick finds nothing after the wait (the partition was revoked
  meanwhile), and when a record is skipped as stale. Both are rare and cost only throughput,
  never the limit.

**Amendment (2026-11-22, Phase 9): sends leave at a fixed offset after their token.**

*Measurement.* The load test (`make loadtest`: 30 000 notifications, 10 000 per channel) counts
the requests the mock provider receives in each wall-clock second. The limiter granted 100
tokens per second (the burst drained at 99/s), but the busiest second at the provider held 102
to 103 requests per channel. The claim sits between the token and the send and takes anywhere
from a few to 50 ms, so sends left unevenly and a few slid into the neighbouring second.

*Decision.* A send leaves exactly 50 ms (`sendOffset`) after its token: the worker sleeps for
whatever the claim did not use. A claim that takes longer still gets a fresh token, which then
gets the same offset. Sends now leave as evenly spaced as the tokens.

*Result.* The busiest second dropped to 100 for most channels and runs, and 101 in a few (a
few milliseconds of jitter on the HTTP path, with slots exactly 10 ms apart). The load test
reports 101 as a warning rather than hiding it; a provider that enforces a strict per-second
window could answer one 429 now and then, which the fleet cool-down handles. Every send gets
up to 50 ms more latency (high priority with no backlog: p50 113 ms before, 160 ms after).

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

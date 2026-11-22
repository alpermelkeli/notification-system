# Capacity planning

Back-of-the-envelope estimates that justify the architecture (ADR-0001) and the scaling
strategy (ADR-0008). Numbers are deliberately rounded. The load test (`make loadtest`) checked
them in Phase 9; the results are under [Measured](#measured).

## 1. The throughput ceiling is set by the provider, not by us

| Quantity | Calculation | Result |
|----------|-------------|--------|
| Delivery rate per channel | requirement FR-2.2 | **100 msg/s** |
| Total delivery rate | 3 channels × 100 msg/s | **300 msg/s** |
| Max deliveries per day | 300 × 86 400 s | **≈ 26 M/day** |
| "Millions per day" (assumed average 5 M/day) | 5 M / 86 400 s | ≈ 58 msg/s average (≈ 20% of ceiling) |

**Consequence:** adding API replicas, workers or database capacity cannot increase delivery
throughput. The design goal is therefore to **absorb bursts safely and deliver them in the
right order**, not to deliver faster.

### Burst drain time
A flash-sale campaign of **2 M SMS** submitted at once:

```
drain time = 2 000 000 / 100 msg/s = 20 000 s ≈ 5.6 hours
```

Implications:
- The backlog must live in durable storage. It sits in Kafka (7-day retention) and PostgreSQL,
  never in worker memory. Worker buffers are bounded to ~50 records per priority (ADR-0002).
- Priority is essential: an OTP submitted during that campaign must not wait 5.6 hours. With the
  capped-aging scheduler (ADR-0006), a `high` message waits at most for the next rate-limit
  token (≤ 10 ms at 100 msg/s), plus pipeline latency.
- If 5.6 hours is not acceptable for the business, the only lever is a **higher provider quota
  or additional providers**. Each gets its own limiter key (ADR-0006).

## 2. Ingestion (API → PostgreSQL)

| Quantity | Estimate |
|----------|----------|
| Batch size | up to 1 000 notifications per request (FR-1.2) |
| Insert path | single multi-row `INSERT` / `COPY` per batch, one transaction |
| PostgreSQL bulk-insert capacity (single primary, modest hardware) | ~10–20 k rows/s |
| Target ingestion | ≥ 5 k notifications/s sustained, short bursts higher |
| Ingestion vs delivery | ≥ 16× faster than the 300 msg/s delivery ceiling |

Ingestion headroom is what lets the system accept a 2 M campaign in minutes and drain it over
hours.

## 3. PostgreSQL write load

Writes per **successfully delivered** notification:

| Step | Writes |
|------|--------|
| API insert (`pending`) + history row | 2 |
| Dispatcher → `queued` + history row | 2 |
| Worker claim → `processing` + history row | 2 |
| Outcome → `sent` + history row | 2 |
| **Total** | **8** |

| Scenario | Calculation | Writes/s |
|----------|-------------|----------|
| Average (58 msg/s) | 58 × 8 | ≈ 460 |
| Ceiling (300 msg/s) | 300 × 8 | **≈ 2 400** |
| Each retry adds | `retrying` + `queued` + `processing` + history | +6 per retry |

Retries cannot amplify provider traffic beyond 100 msg/s per channel (the limiter caps all
sends). A retry storm reduces *goodput*, not safety.

**Verdict:** ~2.4 k writes/s of small rows is comfortably within a single PostgreSQL primary.
**No write sharding is needed.**

## 4. Storage

| Item | Estimate |
|------|----------|
| `notifications` row incl. indexes (avg content ~300 B) | ~1.2 KB |
| History rows (4 × ~150 B incl. index) | ~0.6 KB |
| **Per notification** | **≈ 2 KB** |

| Volume | Per day | 30-day retention |
|--------|---------|------------------|
| Average (5 M/day) | ≈ 10 GB | ≈ 300 GB |
| Ceiling (26 M/day) | ≈ 52 GB | ≈ 1.5 TB |

**Consequences (implemented in migrations, Phase 3):**
- Partition `notifications` and `notification_status_history` by `created_at`, one partition per
  day. Retention is enforced by **dropping old partitions**, which is instant and causes no
  vacuum storm, instead of `DELETE`.
- `fillfactor = 80` on `notifications`. Every transition changes `status`, which appears in the
  partial-index predicates and in the `(status, id)` index, so these updates are **not** HOT and
  do add index entries. The 20% free space still lets the new row version land on the same heap
  page, which keeps a notification's versions together and lets page pruning reclaim dead
  versions before vacuum runs. (Corrected in Phase 3; the first version of this document claimed
  HOT updates.)
- Partitioning constraint: on a partitioned table, unique indexes must include the partition
  key. Therefore:
  - The primary key is `(id, created_at)`. Ids are **UUIDv7** (time-ordered), and the
    claim-check carries `created_at`, so worker lookups prune to a single partition.
  - Idempotency keys live in a separate, non-partitioned `idempotency_keys` table with its own
    TTL cleanup (ADR-0004).
- Partial index for the dispatcher:
  `(next_attempt_at) WHERE status IN ('pending','retrying')`. It stays tiny because only due
  and not-yet-due work is in it.

## 5. Kafka

| Item | Estimate |
|------|----------|
| Work message (claim-check + headers) | ~300 B |
| Work topics at ceiling | 26 M × 300 B ≈ 7.8 GB/day → × RF 3 ≈ 23 GB/day |
| Work topics, 7-day retention | ≈ 165 GB cluster-wide |
| Status events (≈ 4 per notification, ~250 B) | 26 M × 4 × 250 B ≈ 26 GB/day → × RF 3 ≈ 78 GB/day |
| Status events, 3-day retention | ≈ 235 GB cluster-wide |
| Message rate (work + events) at ceiling | ~300 + ~1 200 msg/s, trivial for Kafka |

A 3-broker cluster with modest disks (≥ 500 GB each) has ample headroom. Partition counts
(6/6/3 per channel) are sized for consumer parallelism, not for throughput.

## 6. Worker concurrency (Little's law)

```
in-flight requests = arrival rate × latency
                   = 100 msg/s × 0.2 s (p50 provider latency)  = 20
                   = 100 msg/s × 1.0 s (p99 provider latency)  = 100
```
- The default sender pool is 20 per channel (ADR-0006). Size it as `rate × p99 latency` for a
  slow provider. It is configurable.
- One worker replica per channel can sustain the full 100 msg/s. Extra replicas add **fault
  tolerance**, not throughput.

## 7. Redis (rate limiter)

| Operation | Rate at ceiling |
|-----------|-----------------|
| GCRA `Allow` (Lua) | 300/s |
| Cool-down key check | 300/s |
| **Total** | **≈ 600 ops/s** (a single Redis handles ~100 k ops/s) |

## 8. Dispatcher

| Item | Estimate |
|------|----------|
| Batch per transaction | 500 rows |
| Transaction time (select + publish with `acks=all` + update) | ~20–50 ms |
| Capacity per instance | ≈ 10 k–25 k rows/s |

This is far above the delivery ceiling. Run 2 instances for availability, not for capacity.

## 9. Database connections

| Component | Replicas (example prod) | Pool | Connections |
|-----------|-------------------------|------|-------------|
| API | 6 | 10 | 60 |
| Workers | 3 channels × 2 | 10 | 60 |
| Dispatcher | 2 | 5 | 10 |
| **Total** | | | **≈ 130** |

- Route the API and workers through **PgBouncer** (transaction pooling) so replica count can
  grow without exhausting `max_connections`.
- The dispatcher connects **directly**: `LISTEN/NOTIFY` needs a session-level connection, which
  transaction pooling breaks.

## 10. Latency budget for a `high` notification (no high backlog)

| Hop | Budget |
|-----|--------|
| API validate + insert + `NOTIFY` | ~5–10 ms |
| Dispatcher wake-up + claim | ~10–50 ms |
| Kafka publish (`acks=all`) | ~5–15 ms |
| Consumer fetch (`fetch.max.wait` 50 ms) | ≤ 50 ms |
| Scheduler + rate-limit token | ≤ 10 ms |
| Provider call | ~200 ms (p50) |
| **End-to-end** | **≈ 300 ms p50, target < 1 s p99** |

## Measured

`make loadtest` ([Testing](testing.md#load-test)) on a laptop (Apple Silicon, Docker Desktop),
against the compose stack and the mock provider (50–150 ms latency instead of the 200 ms assumed
above): 30 000 `low` notifications, 10 000 per channel, in 30 batches of 1000, 4 at a time.

| Estimate | Planned | Measured: 1 API, 1 worker | Measured: 3 APIs, 3 workers |
|----------|---------|---------------------------|-----------------------------|
| Ingestion (§2) | ≥ 5 000/s | 27 000–40 000/s | 20 000–23 000/s |
| Dispatcher (§8, 2 instances) | 10 000–25 000 rows/s | 18 000–21 000 rows/s | 10 000–14 000 rows/s |
| Delivery per channel (§1) | 100/s | 98.7–99.3/s | 98.7–99.0/s |
| Busiest second at the provider | ≤ 100 | 100–101 | 100–101 |
| Drain of 10 000 per channel (§1) | 100 s | 101–102 s | 102 s |
| `high`, no backlog, p50 / p99 (§10) | ≈ 300 ms / < 1 s | 160 / 215 ms | 170 / 210–230 ms |
| `high` right behind the burst, p99 (§1, §10) | < 1 s | 670–850 ms | 1.1–1.6 s |

- **Rate limit.** The first run showed 102–103 requests in one second at the provider while the
  limiter granted exactly 100 per second; the variable claim time between token and send bunched
  the sends. Sends now leave at a fixed offset after their token ([ADR-0006](adr/0006-rate-limiting-and-priority-scheduling.md),
  Phase 9 amendment). A second at 101 remains now and then (jitter on the HTTP path).
- **Scaling.** Three workers deliver no faster than one (§6): extra replicas add fault
  tolerance. With one of three workers stopped 45 s into the drain, every notification was still
  sent, the rate stayed at the limit and the drain took the same 102 s.
- **`high` behind a burst.** Almost all of that latency is the wait for the dispatcher (p99
  900 ms of 1.1 s); from `queued` to `sent` takes p99 220 ms. The API accepted the burst faster
  than the dispatcher publishes, and the dispatcher works in FIFO order (ADR-0003), so a `high`
  notification created right after the burst waits behind it.

# 0008. Scalability and deployment topology

- **Status:** accepted
- **Date:** 2026-11-21

## Context
The business needs millions of notifications per day with burst traffic. The supporting numbers
are in [`docs/capacity-planning.md`](../capacity-planning.md). Key findings:
- **Delivery throughput is capped by provider quotas** at 3 × 100 msg/s ≈ 26 M/day. No amount
  of infrastructure raises it.
- At the ceiling, PostgreSQL sees ~2.4 k writes/s and ~10–52 GB/day of data.
- Bursts (e.g. 2 M SMS ≈ 5.6 h to drain) must be absorbed durably and drained in priority order.

We must decide what scales horizontally, what scales vertically, what triggers autoscaling,
and what the production topology looks like. We must also *demonstrate* horizontal scaling
locally.

## Decision

### Scaling model per component
| Component | State | Scales by | Autoscaling signal | Limit / note |
|-----------|-------|-----------|--------------------|--------------|
| **Load balancer** | stateless | managed (ALB) / nginx locally | n/a | WebSocket upgrade passthrough; no sticky sessions needed |
| **API** | stateless | horizontal, N replicas | HPA on CPU (70%) and RPS | Every instance consumes all `notification.events`, so any instance can serve any WebSocket client |
| **Dispatcher** | stateless | horizontal (`SKIP LOCKED`) | none, fixed at 2 for HA | One instance handles ≥ 10 k rows/s, far above the 300 msg/s ceiling |
| **Workers** | stateless | horizontal per channel (consumer group) | KEDA on **consumer lag** of the channel's topics | Max useful replicas = partitions per topic (low = 3). **More replicas ≠ more throughput.** The Redis limiter keeps the channel at 100 msg/s. Replicas add fault tolerance and send concurrency for slow providers |
| **Kafka** | stateful | brokers + partitions | manual | 3 brokers, RF = 3, `min.insync.replicas=2` |
| **Redis** | stateful (tiny) | Sentinel (primary + replica) | none | ~600 ops/s at ceiling |
| **PostgreSQL** | stateful | **vertical + read replica + partitioning** | none | See below |

### PostgreSQL: one writable primary, no sharding
- **Why one primary is enough:** ~2.4 k small writes/s at the delivery ceiling is well within a
  single primary. Write sharding (Citus, app-level) would add major complexity for capacity we
  can never use, because the provider quota caps the write rate.
- **High availability:** primary + **synchronous standby** with automatic failover (Patroni or
  managed Multi-AZ). RPO ≈ 0, RTO < 1 min.
- **Read scaling:** an **asynchronous read replica** serves `GET /v1/notifications/{id}`, batch
  status and list queries. Writes and the dispatcher/worker paths always use the primary.
  Replica lag (typically < 1 s) is acceptable for status reads. Read-your-writes is provided by
  returning the created resource in the `202` response.
- **Connection pooling:** API and workers go through **PgBouncer** (transaction mode). The
  dispatcher connects directly, because `LISTEN/NOTIFY` needs a session connection.
- **Data lifecycle:** daily partitions by `created_at` with partition-drop retention
  (default 30 days), `fillfactor=80` for HOT updates, and a partial index for due work. The
  primary key is `(id, created_at)` with UUIDv7 ids, and idempotency keys are in a separate
  table (ADR-0004).

### What changes at 10× (if provider quotas rise to ~1 000 msg/s per channel)
- ~24 k writes/s: batch status updates per worker (multi-row `UPDATE ... FROM (VALUES ...)`),
  move status history to an append-only path (Kafka → ClickHouse / S3) and keep only the
  current state in PostgreSQL.
- Beyond that, shard `notifications` by `id` (Citus). The design keeps every hot-path query
  keyed by `id`, so sharding stays possible.

### Local demonstration (docker-compose)
- An **nginx** load balancer (`lb`, port 8080) proxies HTTP and WebSocket traffic to the `api`
  service. Docker DNS resolves `api` to all replicas.
- Horizontal scaling is shown with:
  ```bash
  docker compose up --build --scale api=3 --scale worker=3
  ```
  The README shows requests being spread across API replicas (`X-Instance-ID` response header)
  and Kafka partitions being split among workers (per-instance metrics), while total sends stay
  at ≤ 100 msg/s per channel.
- Production manifests (Kubernetes HPA/KEDA) are **documented, not shipped.** They are out of
  scope for a 12 h assessment. The deployment diagram shows the target topology.

## Consequences
- ✅ Scaling claims are backed by numbers (capacity planning) and demonstrated locally (nginx +
  `--scale`).
- ✅ Each autoscaling signal matches what actually constrains that component: CPU/RPS for the
  API, lag for workers. The provider quota is acknowledged as the true ceiling.
- ⚠️ nginx resolves upstream replicas at start-up. After changing `--scale`, restart `lb`. A
  production setup uses a managed LB or service discovery.
- ⚠️ Read-replica lag means a status read can be slightly stale. This is documented in the API
  docs.

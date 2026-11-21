# 0001. Architecture overview: PostgreSQL as source of truth, Kafka as delivery backbone

- **Status:** accepted
- **Date:** 2026-11-21

## Context
We need an event-driven notification system that accepts single and batch requests (FR-1.x),
processes them asynchronously with priorities and a hard rate limit of 100 msg/s per channel
(FR-2.x), retries intelligently (FR-3.x) and exposes status and metrics (FR-4.x). Bonus
features include scheduled delivery (B-2), WebSocket status updates (B-4) and tracing (B-5).

Forces that shape the design:
- **The rate limit sets the throughput ceiling:** 3 channels × 100 msg/s = 300 msg/s
  ≈ 26M notifications/day. Ingestion can be far faster than delivery, so the hard part is
  **absorbing bursts** (flash sales) while keeping high-priority traffic (OTP) unaffected.
- The system needs **per-message** semantics: priority, delayed retry, scheduling, cancellation,
  status queries, filtering and pagination. All of these require a durable, queryable record per
  notification, so a relational store is mandatory regardless of the broker choice.
- Kafka has no native per-message priority, delayed delivery, scheduling or cancellation
  (share groups, KIP-932, add per-record acknowledgement in Kafka 4.2 but still no delayed
  redelivery or dead-lettering).

## Options considered
1. **PostgreSQL only (`FOR UPDATE SKIP LOCKED` as the queue).** Fewest moving parts, transactional
   enqueue, trivial cancel/schedule. Weak "event-driven" story, workers poll the database, no
   event stream for other consumers.
2. **PostgreSQL + RabbitMQ.** Native priority (classic queues only) and DLX-based retries, but
   priority + per-message TTL interact badly (head-of-line), cancellation still needs a DB
   check, dual-write problem.
3. **PostgreSQL + Redis Streams.** Lightweight, but delayed retries need a separate sorted set
   and mover process, and durability is weaker.
4. **Kafka-centric (Kafka as the only queue).** Priority via topics, retries via retry-topic
   chains with partition pausing, but scheduling and cancellation still need a database, plus
   a dual-write/outbox problem. Kafka would be forced into jobs it is not designed for.
5. **PostgreSQL decides *when*, Kafka moves *what* (chosen).**

## Decision
- **PostgreSQL is the single source of truth** for notifications, batches and their state.
  It also owns everything time-based: `next_attempt_at` drives initial dispatch, scheduled
  delivery and retry backoff.
- **A dispatcher** claims due rows (`SKIP LOCKED`), publishes them to Kafka and marks them
  `queued` (see ADR-0003). It wakes up via `LISTEN/NOTIFY`, with a polling fallback.
- **Kafka is the delivery backbone.** It buffers bursts, decouples dispatch from delivery,
  isolates channels and priorities by topic (ADR-0002) and carries status events
  (`notification.events`) for real-time consumers such as WebSocket.
- **Channel workers** consume from Kafka, enforce priority and the shared rate limit
  (ADR-0006), call the provider, and record the outcome (ADR-0005).
- **Redis** provides the distributed per-channel rate limiter.
- **One Go binary with three modes:** `api`, `dispatcher` and `worker`. Each mode scales
  independently in docker-compose.

```
client → API → PostgreSQL ← dispatcher → Kafka (channel × priority topics) → workers → provider
                    ↑                                                          │
                    └──────────── status updates (guarded transitions) ────────┘
                                   workers → notification.events → WebSocket / metrics
```

## Consequences
- ✅ Kafka handles distribution, buffering and event streaming, which it is designed for.
  PostgreSQL handles time, state and queries, which it is designed for.
- ✅ Scheduling and retry share one mechanism, so B-2 comes almost for free.
- ✅ Channels are bulkheaded: a slow email provider cannot delay SMS.
- ⚠️ More infrastructure (Kafka + Redis) and one extra process (dispatcher) than option 1.
- ⚠️ The dispatcher adds a small latency hop, kept sub-second via `LISTEN/NOTIFY`.
- ⚠️ PostgreSQL write volume is ~3 updates per notification (~1k writes/s at peak), which is
  well within a single instance. At much larger scale, partition the table by time and move
  hot-path status updates into batched writes.

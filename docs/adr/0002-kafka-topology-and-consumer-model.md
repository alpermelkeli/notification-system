# 0002. Kafka topology and consumer model

- **Status:** accepted
- **Date:** 2026-11-21

## Context
ADR-0001 makes Kafka the delivery backbone. We need priority isolation (FR-2.3), per-channel
rate limiting and circuit breaking (FR-2.2, B-1), horizontal scaling of workers (FR-2.1) and an
event stream for status consumers (B-4). Kafka has no per-message priority.

## Options considered
- **Topic layout**
  1. One topic per priority, with the channel carried in the key or a header. Fewer topics, but
     pausing a single channel (circuit breaker) affects the others.
  2. **One topic per channel × priority (chosen).** 9 work topics. Channels are isolated and can
     be paused independently, and each priority can be sized independently.
- **Consumer model**
  1. **Classic consumer groups (chosen).** Mature and well understood. Head-of-line blocking is
     not an issue for us, because retries never happen in-line: a failed send is written back to
     PostgreSQL with a future `next_attempt_at`, and the offset is committed.
  2. Share groups (KIP-932, GA in Kafka 4.2). Per-record ack/release and parallelism beyond the
     partition count. Rejected for now: very new (4.2.1 fixed a critical deadlock), no delayed
     redelivery, no DLQ until KIP-1191, and we don't need in-line redelivery. This is the
     upgrade path if we ever need more consumers than partitions.
- **Client library:** `twmb/franz-go` (pure Go, no cgo, full protocol support including
  `PauseFetchTopics`), compared with `segmentio/kafka-go` (simpler but less complete) and
  `confluent-kafka-go` (cgo/librdkafka).

## Decision
**Topics**

| Topic | Partitions (local) | Key | Purpose |
|-------|-------------------|-----|---------|
| `notifications.{sms,email,push}.high` | 6 | notification id | Work: transactional traffic (OTP, alerts) |
| `notifications.{sms,email,push}.normal` | 6 | notification id | Work: default traffic |
| `notifications.{sms,email,push}.low` | 3 | notification id | Work: bulk / marketing |
| `notifications.dlq` | 3 | notification id | Terminally failed notifications (audit/replay) |
| `notification.events` | 6 | notification id | Status-change events, ordered per notification |

- Replication factor is 1 locally and 3 in production. Retention is 7 days. Topics are created
  by an init job in docker-compose.
- **Partition counts size throughput and parallelism; they do not implement priority.**
  Priority is enforced by the worker scheduler (ADR-0006).
- **Work messages are claim-checks:** `{id, channel, priority, attempt, due_at}` plus headers
  `correlation_id` and `traceparent`. The content is loaded from PostgreSQL inside the claim
  `UPDATE ... RETURNING`, so the payload never diverges from the source of truth.
- **Producer:** idempotent producer, `acks=all`.
- **Consumers:** one consumer group per channel (`workers.sms`, `workers.email`,
  `workers.push`), subscribing to that channel's three priority topics. Kafka distributes the
  channel's partitions (6 + 6 + 3 = 15) across all worker replicas in the group and rebalances
  them when replicas join or leave. Example: with 3 SMS worker replicas, each owns about 5 SMS
  partitions. The range assignor splits *each topic* across members, so every replica owns
  partitions from all three priorities (as long as there are no more replicas than a topic's
  partition count) and can apply priority locally.

**Consumption mechanics (per worker replica, per channel)**
- **Batched fetch, routed by topic.** One franz-go client per channel polls its three topics.
  `PollFetches` returns mixed batches (many records across partitions and topics), and a router
  pushes each record into the buffer of its priority (ADR-0006).
- **Bounded memory, backlog stays in Kafka.** When a priority buffer reaches its high-water mark,
  the worker calls `PauseFetchTopics` for that topic and calls `ResumeFetchTopics` once it
  drains below the low-water mark. Records already fetched at pause time are kept, so the bound
  is soft. A burst of millions of messages waits in Kafka, not in worker memory.
- **Offset commit via per-partition watermarks.** Records complete out of order because sends
  run concurrently. Auto-commit is disabled. A tracker records completed offsets per partition
  and commits only the **highest contiguous completed offset**. If offsets 5, 6 and 7 are in
  flight and 6 and 7 finish first, nothing is committed until 5 finishes. "Completed" means the
  outcome (or a skip by the guarded claim) has been persisted to PostgreSQL. Commits are flushed
  periodically (default 1 s) and on shutdown.
- **Rebalance handling** (`OnPartitionsRevoked`):
  1. drop buffered, not-yet-started records of the revoked partitions (the new owner re-reads
     them, and the guarded claim filters duplicates),
  2. wait for in-flight sends on those partitions (bounded, default 10 s),
  3. commit their watermarks.

  The cooperative-sticky protocol is not used, because the range assignor is required for
  per-topic spreading.
- The global 100 msg/s per channel is enforced by the shared Redis limiter (ADR-0006), not by
  the partition count. Adding replicas adds resilience and send concurrency, but never exceeds
  the limit.
- **Event consumers** (e.g. the WebSocket hub in each API instance) use a unique group per
  instance starting at `latest`, so every instance sees every event.

## Consequences
- ✅ A channel can be paused (circuit open) without touching other channels.
- ✅ Throughput per channel/priority can be tuned with the partition count.
- ✅ Watermark commits mean a crash never skips an unfinished record. It can only re-deliver
  finished ones, which the guarded claim filters (ADR-0004). The dispatcher's reaper for rows
  stuck in `queued` (ADR-0003) remains a last-resort safety net, not the primary mechanism.
- ⚠️ One slow record holds back its partition's commit (not its processing). After a crash, at
  most the in-flight window is re-read.
- ⚠️ The range assignor uses eager rebalancing, so all partitions briefly stop during a
  rebalance. This is acceptable for a pool of worker replicas that rarely changes.
- ⚠️ The number of active consumers per topic is bounded by the partition count. Switch to
  share groups if that becomes a limit.

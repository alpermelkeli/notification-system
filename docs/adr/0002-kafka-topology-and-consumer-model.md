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
  partition count) and can apply priority locally. Auto-commit is disabled. Offsets are
  committed after the outcome is persisted.
- The global 100 msg/s per channel is enforced by the shared Redis limiter (ADR-0006), not by
  the partition count. Adding replicas adds resilience and send concurrency, but never exceeds
  the limit.
- **Event consumers** (e.g. the WebSocket hub in each API instance) use a unique group per
  instance starting at `latest`, so every instance sees every event.

## Consequences
- ✅ A channel can be paused (circuit open) without touching other channels.
- ✅ Throughput per channel/priority can be tuned with the partition count.
- ⚠️ Committing offsets out of order under concurrent processing could skip unfinished
  records after a crash. This is acceptable because the dispatcher's reaper re-publishes rows
  stuck in `queued` beyond a timeout (ADR-0003).
- ⚠️ The number of active consumers per topic is bounded by the partition count. Switch to
  share groups if that becomes a limit.

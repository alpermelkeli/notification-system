# Delivery Plan

Living document. Update phase status as work lands. Time budget ≈ 12h of focused work.

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Repo bootstrap: README, CLAUDE.md, docs, Claude subagents | done |
| 1 | Architecture decisions (ADRs 0001–0008), capacity planning, architecture + deployment diagrams | done |
| 2 | Skeleton: Go module, layout, config, logging, health, docker-compose (postgres, kafka, redis, nginx lb, mock provider), Makefile, CI | done |
| 3 | Domain + persistence: model, state machine, migrations (daily partitions, partial index, fillfactor, idempotency_keys), `notifier migrate` + compose migrate job, repository | done |
| 4 | API: create / batch / get / cancel / list + validation + OpenAPI | done |
| 5 | Processing engine: dispatcher, Kafka topics, workers, capped-aging scheduler, rate limiter, provider client | done |
| 6 | Delivery & retry: backoff, classification, DLQ, circuit breaker | next |
| 7 | Observability: metrics endpoint, correlation IDs, tracing | |
| 8 | Bonus: scheduling, templates, WebSocket | |
| 9 | Hardening: integration/e2e tests, load test vs capacity-planning numbers, `--scale` demo, README polish, final review | |

## Resolved decisions (Phase 1)
| Question | Decision | ADR |
|----------|----------|-----|
| Queue / broker | PostgreSQL (state + timing) + Kafka (distribution + events) | [0001](adr/0001-architecture-overview.md) |
| Topics & consumers | 9 channel × priority topics, classic consumer groups, franz-go | [0002](adr/0002-kafka-topology-and-consumer-model.md) |
| Dual write | Notifications table is the outbox, FIFO dispatcher + reaper | [0003](adr/0003-dispatch-without-separate-outbox.md) |
| Idempotency | Idempotency-Key + request hash, guarded claim, provider header | [0004](adr/0004-idempotency.md) |
| Retry / DLQ | Out-of-band backoff + jitter, classification, DLQ topic, circuit breaker | [0005](adr/0005-delivery-retry-and-failure-handling.md) |
| Rate limit & priority | Redis GCRA per channel/provider, 429 cool-down, capped-aging scheduler | [0006](adr/0006-rate-limiting-and-priority-scheduling.md) |
| Stack & deployment | Go 1.26, chi, pgx, goose; one binary: api / dispatcher / worker / migrate | [0007](adr/0007-technology-stack.md) |
| Scaling & topology | Per-component scaling model, single PG primary + HA/replica/partitions, nginx + `--scale` demo | [0008](adr/0008-scalability-and-deployment-topology.md) |

## Resolved decisions (later phases)
| Question | Decision | ADR |
|----------|----------|-----|
| Reaper rule for `queued` rows | Compare each row's stored Kafka offset with the consumer group's committed offset instead of a fixed 10-min age; 24 h age fallback. Implemented in Phase 5 (migration 00002 adds `kafka_partition`, `kafka_offset`) | [0003 amendment](adr/0003-dispatch-without-separate-outbox.md) |
| Reaper coordination | No advisory lock: every dispatcher runs the reaper, `SKIP LOCKED` keeps it correct | [0003 amendment](adr/0003-dispatch-without-separate-outbox.md) |
| Claim racing the dispatch commit | Guarded transitions lock the row by key and check guards on the locked version, so the claim waits for the dispatcher's commit | [0003 amendment](adr/0003-dispatch-without-separate-outbox.md) |
| Rate limiter | Reserving GCRA Lua script instead of `redis_rate`; burst default 1 (measured: 100/s peak, ~99% throughput with 1 or 3 workers) | [0006 amendment](adr/0006-rate-limiting-and-priority-scheduling.md), [0007](adr/0007-technology-stack.md) |

## Phase 6 starting point
The Phase 5 worker uses a placeholder outcome policy in `internal/worker/process.go`: any failure
is retried after `RETRY_DELAY` until `MAX_ATTEMPTS`, and a `429` also sets the fleet cool-down.
Phase 6 replaces it with ADR-0005: exponential backoff with jitter, retryable vs. permanent
classification, the DLQ topic and the per-channel circuit breaker (`Release` already exists for
it).

Also from the Phase 5 review: while PostgreSQL is unavailable, every claim fails fast and its
record is committed past, so the workers drain the Kafka backlog and recovery waits for the
reaper (re-publishing in `queued_at` order, not by priority). Phase 6 should back off and retry
the claim on an unavailability error instead, so the watermark holds.

## Open questions
| Phase | Question | Notes |
|-------|----------|-------|
| 6 | Reaped lease at the attempt limit | `ReapExpiredLeases` keeps the attempt counted, so a row reaped at `max_attempts` is dispatched once more. Move it to `failed` at claim time (or in the reaper) when attempts ≥ max. |

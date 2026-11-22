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
| 6 | Delivery & retry: backoff, classification, DLQ, circuit breaker | done |
| 7 | Observability: metrics endpoint, correlation IDs, tracing | done |
| 8 | Bonus: scheduling, templates, WebSocket | done |
| 9 | Hardening: integration/e2e tests, load test vs capacity-planning numbers, `--scale` demo, README polish, final review | next |

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
| Reaped lease at the attempt limit | The reaper fails it (with a dead letter) in the same statement instead of dispatching it again | [0005 amendment](adr/0005-delivery-retry-and-failure-handling.md) |
| Breaker while open | Checked before the pick; records stay buffered and fetching pauses through backpressure, no attempts burned; sends claimed after the trip are released | [0005 amendment](adr/0005-delivery-retry-and-failure-handling.md) |
| Claim during a PostgreSQL outage | Retried with backoff on transient errors so the watermark holds; abandoned below it on shutdown or revoke | [0005 amendment](adr/0005-delivery-retry-and-failure-handling.md) |
| Observability | Per-role Prometheus registries; queue depth and consumer lag sampled by the dispatcher; the request's `traceparent` stored with the row and carried in Kafka headers, so one trace covers every attempt; trace ids in logs | [0009](adr/0009-observability.md) |
| Templates (B-3) | Stored templates (`/v1/templates`), `{{name}}` substitution only, rendered and validated at creation; rows record `template_id` and `template_version` | [0010](adr/0010-templates.md) |
| Status events and WebSocket (B-4) | Rows published best-effort to `notification.events` after each commit; every API instance reads all events without a group and fans them out to `/v1/ws` subscribers of notification and batch ids, with a snapshot on subscribe; `coder/websocket` | [0011](adr/0011-status-events-and-websocket.md) |
| Rate limit at the provider (load test) | Sends leave a fixed 50 ms after their token, so the variable claim time cannot bunch them into one second | [0006 amendment](adr/0006-rate-limiting-and-priority-scheduling.md) |
| `high` waiting behind a burst in the dispatcher (load test) | The dispatcher takes due `high` rows first (migration 00005 adds their partial index) | [0003 amendment](adr/0003-dispatch-without-separate-outbox.md) |
| Readiness of multi-channel workers | `/readyz` checks every channel's Kafka client (`kafka-sms`, `kafka-email`, `kafka-push`) | [0009](adr/0009-observability.md) |

## Phase 9 progress
- `/playground`: live pipeline map, step-by-step narration and ten self-checking scenarios;
  ADR-0011 amended.
- README reduced to an overview; details moved to `docs/how-it-works.md`, `docs/api.md`,
  `docs/configuration.md` and `docs/testing.md`.
- Load test (`make loadtest`) with measured values in `docs/capacity-planning.md`, run on one
  and on three API and worker replicas, including a worker stopped mid-drain. It led to the two
  amendments above.

## Phase 9 starting point
Phase 8's review follow-ups on event ordering (`clock_timestamp()`) and browser origins
(`WS_ALLOWED_ORIGINS`) are done. One remains, not blocking: item keys of a batch sent without a
batch key are checked after template validation, so their retry fails if the template changed
incompatibly (ADR-0010).

## Open questions
| Phase | Question | Notes |
|-------|----------|-------|
| — | None open | |

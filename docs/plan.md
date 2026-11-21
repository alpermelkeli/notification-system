# Delivery Plan

Living document. Update phase status as work lands. Time budget ≈ 12h of focused work.

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Repo bootstrap: README, CLAUDE.md, docs, Claude subagents | done |
| 1 | Architecture decisions (ADRs 0001–0008), capacity planning, architecture + deployment diagrams | done |
| 2 | Skeleton: Go module, layout, config, logging, health, docker-compose (postgres, kafka, redis, nginx lb, mock provider), Makefile, CI | done |
| 3 | Domain + persistence: model, state machine, migrations (daily partitions, partial index, fillfactor, idempotency_keys), `notifier migrate` + compose migrate job, repository | done |
| 4 | API: create / batch / get / cancel / list + validation + OpenAPI | next |
| 5 | Processing engine: dispatcher, Kafka topics, workers, capped-aging scheduler, rate limiter, provider client | |
| 6 | Delivery & retry: backoff, classification, DLQ, circuit breaker | |
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

## Open questions
| Phase | Question | Notes |
|-------|----------|-------|
| 5 | Reaper rule for `queued` rows (ADR-0003: back to `pending` after 10 min) | `storage.RequeueStuck(olderThan)` and its index exist; the policy does not. During a burst, `queued` rows legitimately wait in Kafka for hours (2 M SMS ≈ 5.6 h at 100 msg/s), so a fixed 10-min threshold would re-publish the whole backlog. Options: disable by default, compare against consumer lag / committed offsets, or use a much larger threshold. |
| 4 | Upper bound for `scheduledAt` | A notification scheduled beyond `RETENTION_DAYS` would be dropped with its partition before delivery; reject such requests in validation. |
| 6 | Reaped lease at the attempt limit | `ReapExpiredLeases` keeps the attempt counted, so a row reaped at `max_attempts` is dispatched once more. Move it to `failed` at claim time (or in the reaper) when attempts ≥ max. |

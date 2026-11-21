# Delivery Plan

Living document. Update phase status as work lands. Time budget ≈ 12h of focused work.

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Repo bootstrap: README, CLAUDE.md, docs, Claude subagents | done |
| 1 | Architecture decisions (ADRs 0001..n): stack, queue, storage, retry, rate limit, idempotency | next |
| 2 | Skeleton: Go module, layout, config, logging, health, docker-compose, Makefile, CI | |
| 3 | Domain + persistence: model, state machine, migrations, repository | |
| 4 | API: create / batch / get / cancel / list + validation + OpenAPI | |
| 5 | Processing engine: priority queues, workers, rate limiter, idempotency, provider client | |
| 6 | Delivery & retry: backoff, classification, DLQ, circuit breaker | |
| 7 | Observability: metrics endpoint, correlation IDs, tracing | |
| 8 | Bonus: scheduling, templates, WebSocket | |
| 9 | Hardening: integration/e2e tests, load sanity check, README polish, final review | |

## Open questions (to resolve in Phase 1)
- Queue/broker: Redis (streams / sorted sets), RabbitMQ, Kafka, or Postgres-as-queue (`SKIP LOCKED`)?
- Storage: PostgreSQL (assumed) — confirm.
- Rate limiter scope: per channel global (distributed, Redis) vs. per process.
- Idempotency: client `Idempotency-Key` header + DB unique constraint? provider-side dedupe?
- Deployment shape: single binary with `api` / `worker` modes vs. separate services.
- HTTP framework: stdlib `net/http` (Go 1.22+ routing) vs. chi / gin / echo.

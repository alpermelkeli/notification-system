# CLAUDE.md

Guidance for Claude Code (and other agents) working in this repository.

## Project
Event-driven, multi-channel (SMS / Email / Push) notification system in **Go** — Insider One
Software Engineer Assessment 2026. Goal: production-quality design and code within ~12h of work,
judged on architecture, reliability, code quality, tests, docs and **commit history**.

## Read first
- @docs/requirements.md — every requirement has an ID (FR-x.y, B-x, D-x). Reference IDs in ADRs, PRs and commit bodies; tick the checklist when done.
- @docs/plan.md — current phase and open questions.
- @docs/conventions.md — code, testing, API, DB, git rules.
- `docs/adr/` — accepted ADRs are binding. Don't silently deviate; propose a new ADR instead.

## Working agreement
- **Architecture is decided with the user first.** Don't introduce a new dependency, broker, datastore or framework without an accepted ADR.
- Work phase by phase from `docs/plan.md`; keep changes scoped to the current phase.
- Every commit: builds, `make test` green, `make lint` clean, Conventional Commit message, one logical change.
- Keep `README.md`, `docs/` (API guide, configuration, how it works), the OpenAPI spec and `.env.example` in sync with behavior in the same commit.
- Never call the real webhook.site from tests; use `httptest`.
- Never commit secrets or `docs/assesment.pdf`.

## Subagents (`.claude/agents/`)
| Agent | Owns |
|-------|------|
| `go-api` | HTTP handlers, DTOs, validation, middleware, OpenAPI |
| `go-engine` | Queues, workers, rate limiter, provider client, retry/DLQ, scheduling |
| `go-storage` | Schema, migrations, repositories |
| `go-platform` | Bootstrap, config, logging, metrics, health, tracing, Docker, Makefile, CI |
| `go-tester` | Unit / race / integration / e2e tests (test files only) |
| `reviewer` | Read-only review against ADRs and conventions |

### Parallel work rules
- The main session orchestrates: it defines shared contracts (domain types, interfaces) **first**, commits them, then fans out.
- Give each agent an explicit list of packages it may touch; no two parallel agents own the same package.
- Shared files (`go.mod`, `internal/domain`, `cmd/`, OpenAPI root) are changed only by the main session, or by one designated agent per round.
- Run parallel implementers with `isolation: "worktree"` when their file sets could overlap; merge back and run `make test` before committing.
- Run `reviewer` on the combined diff before each phase commit.

## Commands
```bash
make help          # list all targets
make up            # docker compose up -d --build (API via nginx on :8080)
make up-scaled     # 3 API + 3 worker replicas (scaling demo)
make infra         # only postgres (+ migrate job), kafka, redis, mock provider (for `make run-*`)
make migrate       # apply migrations + create partitions against DATABASE_URL (also migrate-status)
make run-api       # also run-dispatcher, run-worker, run-mockprovider
make test          # go test -race ./... (must be green before every commit)
make test-short    # unit tests only, no Docker (storage integration tests are skipped)
make lint          # golangci-lint v2 (.golangci.yml)
make down / clean  # stop / stop and delete volumes
```

## Layout
```
cmd/notifier/          single binary; subcommands api | dispatcher | worker | migrate | healthcheck | version
cmd/mockprovider/      local fake of webhook.site (fault injection, /stats)
internal/app/          role wiring (RunAPI, RunDispatcher, RunWorker, RunMigrate), shared router, graceful HTTP server
internal/domain/       channels, priorities, status state machine, templates, UUIDv7 ids, domain errors (no I/O)
internal/storage/      PostgreSQL repository: create/batch/idempotency, guarded transitions, list, partitions
migrations/            goose SQL migrations, embedded (forward-only, never edited after merge)
internal/api/          v1 HTTP handlers, templates, request validation, error mapping, Swagger UI (/docs), /ws-demo
api/                   openapi.yaml (spec-first contract, Redocly-linted) embedded as package apispec
internal/config/       env -> typed Config (all errors reported at once)
internal/logging/      slog JSON logger; correlation ID carried in context.Context, trace/span ids added from it
internal/metrics/      Prometheus metric sets per role (HTTP/API, dispatch + queue depth, delivery)
internal/telemetry/    OpenTelemetry setup, HTTP server spans, traceparent in rows and Kafka headers
internal/events/       best-effort status event publisher (notification.events)
internal/ws/           WebSocket hub: reads all status events, serves /v1/ws subscriptions with snapshots
internal/playground/   /playground demo page (embedded HTML/JS/SVG), Prometheus summary, mock provider fault proxy
internal/httpx/        error envelope, JSON helpers, middleware (correlation id, instance id, logging, recovery)
internal/health/       /healthz (liveness) and /readyz (concurrent dependency checks)
internal/platform/     lazy PostgreSQL / Kafka / Redis clients + readiness checks
internal/mockprovider/ mock provider handler
internal/queue/        Kafka contract: topic/group names, claim-check work message, dead letters, status events
internal/dispatcher/   relay (PG -> Kafka, NOTIFY + poll), publisher, reaper (leases, offset rule, age fallback)
internal/worker/       per-channel consumer: priority heaps, capped-aging scheduler, watermarks, circuit breaker, claim/send/outcome
internal/retry/        backoff with equal jitter and the attempt budget (ADR-0005)
internal/ratelimit/    reserving GCRA in Redis + fleet cool-down
internal/provider/     HTTP client of the delivery provider, outcome classification
internal/testinfra/    containers for integration tests (PostgreSQL, Kafka, Redis)
internal/integration/  end-to-end engine tests (containers + httptest provider)
deploy/                nginx LB config, Kafka topic creation script, Prometheus scrape config
docs/                  requirements, plan, conventions, ADRs, capacity planning, diagrams
```

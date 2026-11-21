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
- Keep `README.md`, OpenAPI spec and `.env.example` in sync with behavior in the same commit.
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
make infra         # only postgres, kafka, redis, mock provider (for `make run-*`)
make run-api       # also run-dispatcher, run-worker, run-mockprovider
make test          # go test -race ./... (must be green before every commit)
make test-short    # unit tests only, no Docker
make lint          # golangci-lint v2 (.golangci.yml)
make down / clean  # stop / stop and delete volumes
```

## Layout
```
cmd/notifier/          single binary; subcommands api | dispatcher | worker | healthcheck | version
cmd/mockprovider/      local fake of webhook.site (fault injection, /stats)
internal/app/          role wiring (RunAPI, RunDispatcher, RunWorker), shared router, graceful HTTP server
internal/config/       env -> typed Config (all errors reported at once)
internal/logging/      slog JSON logger; correlation ID carried in context.Context
internal/httpx/        error envelope, JSON helpers, middleware (correlation id, instance id, logging, recovery)
internal/health/       /healthz (liveness) and /readyz (concurrent dependency checks)
internal/platform/     lazy PostgreSQL / Kafka / Redis clients + readiness checks
internal/mockprovider/ mock provider handler
deploy/                nginx LB config, Kafka topic creation script
docs/                  requirements, plan, conventions, ADRs, capacity planning, diagrams
```
Packages added by later phases: `internal/domain`, `internal/storage` + `migrations/` (Phase 3),
`internal/api` + `api/openapi.yaml` (Phase 4), `internal/dispatcher`, `internal/worker`,
`internal/ratelimit`, `internal/provider` (Phases 5–6).

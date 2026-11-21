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
_Filled in during Phase 2 (skeleton)._ Expected:
```bash
docker compose up --build   # full stack
make run-api / make run-worker
make test                   # unit + integration
make lint
make migrate-up
```

## Layout
_Filled in once the skeleton lands (Phase 2)._

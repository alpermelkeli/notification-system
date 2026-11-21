# Notification System

An event-driven, multi-channel notification service written in Go. It accepts notification
requests over HTTP, processes them asynchronously through prioritized queue workers, and delivers
them via SMS, Email and Push providers with rate limiting, retries and real-time status tracking.

> **Status:** 🚧 Bootstrapping — architecture decisions in progress. See [`docs/plan.md`](docs/plan.md).

## Features (target)
- **Notification API:** single and batch create (up to 1000), status by ID / batch ID, cancel, filtered + paginated listing
- **Processing engine:** async workers, high / normal / low priority queues, 100 msg/s per channel rate limit, content validation, idempotency
- **Delivery & retry:** exponential backoff with jitter, failure classification, dead-letter handling
- **Observability:** metrics endpoint (queue depth, success/failure rates, latency), structured logs with correlation IDs, health checks
- **Bonus:** scheduled notifications, templates, WebSocket status updates, distributed tracing, CI/CD

## Quick start
_Coming in Phase 2._
```bash
cp .env.example .env        # set PROVIDER_URL to your webhook.site URL
docker compose up --build
```

## Architecture
_To be documented after the architecture decisions are accepted._
See [`docs/adr/`](docs/adr/) for Architecture Decision Records.

## API
_OpenAPI spec and examples coming in Phase 4._

## Configuration
_Environment variables documented in Phase 2._

## Testing
_`make test` — coming in Phase 2._

## Project docs
| Path | Contents |
|------|----------|
| [`docs/requirements.md`](docs/requirements.md) | Requirements & traceability checklist |
| [`docs/plan.md`](docs/plan.md) | Delivery plan & open questions |
| [`docs/conventions.md`](docs/conventions.md) | Engineering conventions |
| [`docs/adr/`](docs/adr/) | Architecture Decision Records |
| [`.claude/agents/`](.claude/agents/) | Claude Code subagents used for parallel development |

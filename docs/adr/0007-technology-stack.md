# 0007. Technology stack

- **Status:** accepted
- **Date:** 2026-11-21

## Context
The brief requires Go (1.2x), docker-compose one-command setup (D-3), an OpenAPI spec (D-4),
versioned migrations (D-5) and a single-command test suite (D-6). We prefer the standard
library and small, well-maintained libraries over frameworks.

## Decision
| Concern | Choice | Why |
|---------|--------|-----|
| Language | Go 1.25 | Latest stable; `log/slog`, enhanced `net/http` routing |
| HTTP router | `go-chi/chi/v5` | `net/http`-compatible, idiomatic middleware |
| PostgreSQL | PostgreSQL 17 + `jackc/pgx/v5` (pgxpool) | Fast native driver, `LISTEN/NOTIFY`, `COPY` for batches |
| Migrations | `pressly/goose/v3` (SQL files) | Versioned SQL, embeddable, runs as a compose job |
| Kafka | Apache Kafka 4.x (KRaft, single node locally) + `twmb/franz-go` | ADR-0002 |
| Redis | Redis 7 + `redis/go-redis/v9` + `go-redis/redis_rate/v10` | ADR-0006 |
| Validation | `go-playground/validator/v10` + custom channel rules | Declarative struct validation |
| Circuit breaker | `sony/gobreaker/v2` | Small and proven |
| API contract | Spec-first `api/openapi.yaml`, served with Swagger UI | The contract is reviewed before code |
| Logging | `log/slog` (JSON) | Standard library, structured, carries `correlation_id` |
| Metrics | `prometheus/client_golang` at `/metrics` | Industry standard; queue depth, rates, latency histograms |
| Tracing | OpenTelemetry SDK → OTLP → Jaeger | B-5; trace context propagated via Kafka headers |
| Tests | `testing` + `testcontainers-go` (Postgres, Kafka, Redis) + `httptest` | Real dependencies in integration tests |
| Lint | `golangci-lint` | CI gate |
| CI | GitHub Actions | B-6 |

**Deployment shape:** a single binary, `notifier`, with subcommands `api`, `dispatcher`,
`worker` and `migrate`. docker-compose runs postgres, kafka, redis, the migrate and
topic-init jobs, an nginx load balancer in front of api (ADR-0008), api, dispatcher, worker,
jaeger and a mock provider (ADR-0006). Ids are UUIDv7 (`google/uuid`).

## Consequences
- ✅ A small, mainstream dependency set that reviewers will recognise.
- ⚠️ Testcontainers makes integration tests require Docker. They are skipped under `-short`
  so unit tests run anywhere.
- Any new dependency outside this list needs a new ADR.

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
| Language | Go 1.26 (`go 1.26` in `go.mod`) | Minimum required by current releases of franz-go, go-redis and client_golang. Go 1.25 left support when 1.27 shipped. `log/slog`, enhanced `net/http` routing |
| HTTP router | `go-chi/chi/v5` | `net/http`-compatible, idiomatic middleware |
| PostgreSQL | PostgreSQL 17 + `jackc/pgx/v5` (pgxpool) | Fast native driver, `LISTEN/NOTIFY`, `COPY` for batches |
| Migrations | `pressly/goose/v3` (SQL files) | Versioned SQL, embeddable, runs as a compose job |
| Kafka | Apache Kafka 4.x (KRaft, single node locally) + `twmb/franz-go` | ADR-0002 |
| Redis | Redis 7 + `redis/go-redis/v9` (`go-redis/redis_rate/v10` planned, replaced in Phase 5) | ADR-0006 |
| Validation | Hand-written rules in `internal/domain` (amended, see below) | Channel-specific rules with per-field messages |
| Circuit breaker | `sony/gobreaker/v2` | Small and proven |
| API contract | Spec-first `api/openapi.yaml`, served with Swagger UI, linted with Redocly CLI in CI | The contract is reviewed before code |
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

**Amendment (2026-11-21, Phase 2):** the language row originally said Go 1.25. Bootstrapping the
module showed that the current releases of franz-go, go-redis and client_golang require Go 1.26,
so the target moved to 1.26 instead of pinning older library versions.

**Amendment (2026-11-21, Phase 4):** `go-playground/validator` was dropped before it was ever
added. Almost every rule is channel-specific (E.164 vs. email address vs. device token, SMS
segment counting with GSM-7/UCS-2 detection), which struct tags cannot express, and the API needs
violations as `field` paths such as `notifications[3].content`. Plain Go functions in
`internal/domain` do both with one dependency fewer. Swagger UI is loaded from a pinned CDN
build (`swagger-ui-dist@5.17.14`) rather than embedded, so it adds no Go dependency.

**Amendment (2026-11-21, Phase 5):** `twmb/franz-go/pkg/kadm` was added. It is the admin client
of the franz-go project, shipped as its own module, and the reaper uses it to read committed
offsets (ADR-0003). It is pinned to v1.20.0, the release paired with franz-go v1.22.3. The
listed `go-redis/redis_rate/v10` was dropped: measurements showed its try-and-retry model loses
send slots under contention, and a 30-line reserving GCRA script on plain go-redis replaces it
(ADR-0006 amendment). No testcontainers module was added for Kafka or Redis: the Kafka module
only supports Confluent images, so `internal/testinfra` starts the compose images
(`apache/kafka`, `redis`) as generic containers.

## Consequences
- ✅ A small, mainstream dependency set that reviewers will recognise.
- ⚠️ Testcontainers makes integration tests require Docker. They are skipped under `-short`
  so unit tests run anywhere.
- Any new dependency outside this list needs a new ADR.

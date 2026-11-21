# Requirements & Traceability

Source: Insider One — Software Engineer Assessment 2026, "Event-Driven Notification System"
(original PDF lives at `docs/assesment.pdf`, git-ignored). Language: **Go 1.2x**. Budget: ~12h.

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done. Keep the "Where" column pointing at code/tests.

## Business context
Millions of notifications/day across SMS, Email, Push. Must absorb burst traffic (flash sales,
breaking news), retry failed deliveries intelligently, and expose delivery status to internal
teams and API consumers.

## Functional

### FR-1 Notification Management API
| ID | Requirement | Status | Where |
|----|-------------|--------|-------|
| FR-1.1 | Create notification: recipient, channel, content, priority | [x] | `POST /v1/notifications`: `internal/api/handlers.go`, `internal/storage/create.go` |
| FR-1.2 | Batch create, up to **1000** per request | [x] | `POST /v1/notifications/batch`, one transaction, all-or-nothing validation |
| FR-1.3 | Query status by notification ID or batch ID | [x] | `GET /v1/notifications/{id}`, `/history`, `GET /v1/batches/{id}` |
| FR-1.4 | Cancel pending notifications | [x] | `POST /v1/notifications/{id}/cancel`, guarded on pending/queued/retrying |
| FR-1.5 | List with filters (status, channel, date range) + pagination | [x] | `GET /v1/notifications`: status/channel/batch/date filters, opaque cursor on the UUIDv7 id |

### FR-2 Processing Engine
| ID | Requirement | Status | Where |
|----|-------------|--------|-------|
| FR-2.1 | Async processing via queue workers | [x] | `internal/dispatcher` (relay, reaper), `internal/worker`, `internal/integration` |
| FR-2.2 | Rate limit: max **100 msg/s per channel** | [x] | `internal/ratelimit` (reserving GCRA in Redis, shared by all workers), measured in compose |
| FR-2.3 | Priority queues: high / normal / low | [x] | 9 channel × priority topics, `internal/worker/scheduler.go` (capped aging + minimum share) |
| FR-2.4 | Content validation (char limits, required fields) | [x] | `internal/domain/validate.go` (per-channel rules), `internal/api/request.go` |
| FR-2.5 | Idempotency — no duplicate sends | [x] | `Idempotency-Key` in `internal/api`, `internal/storage/idempotency.go`, guarded + fenced `Claim`, provider `Idempotency-Key` header (`internal/provider`) |

### FR-3 Delivery & Retry Logic
Left open by the brief ("thinking and design by candidate"). Our design must be documented in an ADR.
| ID | Requirement | Status | Where |
|----|-------------|--------|-------|
| FR-3.1 | Retry policy (backoff + jitter, max attempts) | [x] | `internal/retry` (exponential, equal jitter), `internal/worker/process.go` (`settle`), reaper fails expired leases at the limit |
| FR-3.2 | Retryable vs. permanent failure classification | [x] | `internal/provider/classify.go` |
| FR-3.3 | Dead-letter handling / terminal failed state | [x] | `failed` + `last_error`, `notifications.dlq` claim-checks (`internal/queue/deadletter.go`), replay via `POST /v1/notifications/{id}/retry` |
| FR-3.4 | Status lifecycle & state machine | [x] | `internal/domain/status.go`, guarded transitions + history in `internal/storage/transitions.go` |

### FR-4 Observability
| ID | Requirement | Status | Where |
|----|-------------|--------|-------|
| FR-4.1 | Real-time metrics endpoint: queue depth, success/failure rates, latency | [x] | `/metrics` on every role (`internal/metrics`); queue depth and consumer lag sampled by `internal/dispatcher/depth.go`; delivery outcomes, provider and end-to-end latency in `internal/worker` |
| FR-4.2 | Structured logging with correlation IDs | [x] | `internal/logging` (`correlation_id`, `trace_id`, `span_id`), `internal/httpx/middleware.go`, Kafka `correlation_id` header → worker logs and provider `X-Correlation-ID` |
| FR-4.3 | Health check endpoint | [x] | `internal/health`, `/healthz` + `/readyz` on every role |

## External provider (webhook.site)
- `POST https://webhook.site/{uuid}` with body `{ "to": "...", "channel": "sms", "content": "..." }`
- Configure webhook.site to respond **202** with `{ "messageId": "uuid", "status": "accepted", "timestamp": "ISO8601" }`
- Provider URL must be configurable via env var (never hard-coded).

## Bonus (optional, viewed favorably)
| ID | Feature | Status |
|----|---------|--------|
| B-1 | Failure handling (circuit breaker, DLQ, replay) | [x] |
| B-2 | Scheduled notifications (future delivery) | [x] |
| B-3 | Template system with variable substitution | [x] |
| B-4 | WebSocket real-time status updates | [x] |
| B-5 | Distributed tracing | [x] |
| B-6 | GitHub Actions CI/CD (test + lint) | [x] |

## Deliverables
| ID | Deliverable | Status |
|----|-------------|--------|
| D-1 | Repo with **clean commit history** | [~] |
| D-2 | README: setup, architecture overview, API examples | [~] |
| D-3 | `docker-compose up` one-command setup | [~] |
| D-4 | Swagger / OpenAPI spec | [x] |
| D-5 | Versioned DB migrations | [x] |
| D-6 | Test suite runnable with a single command | [x] |

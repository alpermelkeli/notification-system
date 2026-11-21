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
| FR-1.1 | Create notification: recipient, channel, content, priority | [ ] | |
| FR-1.2 | Batch create, up to **1000** per request | [ ] | |
| FR-1.3 | Query status by notification ID or batch ID | [ ] | |
| FR-1.4 | Cancel pending notifications | [ ] | |
| FR-1.5 | List with filters (status, channel, date range) + pagination | [ ] | |

### FR-2 Processing Engine
| ID | Requirement | Status | Where |
|----|-------------|--------|-------|
| FR-2.1 | Async processing via queue workers | [ ] | |
| FR-2.2 | Rate limit: max **100 msg/s per channel** | [ ] | |
| FR-2.3 | Priority queues: high / normal / low | [ ] | |
| FR-2.4 | Content validation (char limits, required fields) | [ ] | |
| FR-2.5 | Idempotency — no duplicate sends | [ ] | |

### FR-3 Delivery & Retry Logic
Left open by the brief ("thinking and design by candidate"). Our design must be documented in an ADR.
| ID | Requirement | Status | Where |
|----|-------------|--------|-------|
| FR-3.1 | Retry policy (backoff + jitter, max attempts) | [ ] | |
| FR-3.2 | Retryable vs. permanent failure classification | [ ] | |
| FR-3.3 | Dead-letter handling / terminal failed state | [ ] | |
| FR-3.4 | Status lifecycle & state machine | [ ] | |

### FR-4 Observability
| ID | Requirement | Status | Where |
|----|-------------|--------|-------|
| FR-4.1 | Real-time metrics endpoint: queue depth, success/failure rates, latency | [ ] | |
| FR-4.2 | Structured logging with correlation IDs | [ ] | |
| FR-4.3 | Health check endpoint | [ ] | |

## External provider (webhook.site)
- `POST https://webhook.site/{uuid}` with body `{ "to": "...", "channel": "sms", "content": "..." }`
- Configure webhook.site to respond **202** with `{ "messageId": "uuid", "status": "accepted", "timestamp": "ISO8601" }`
- Provider URL must be configurable via env var (never hard-coded).

## Bonus (optional, viewed favorably)
| ID | Feature | Status |
|----|---------|--------|
| B-1 | Failure handling (circuit breaker, DLQ, replay) | [ ] |
| B-2 | Scheduled notifications (future delivery) | [ ] |
| B-3 | Template system with variable substitution | [ ] |
| B-4 | WebSocket real-time status updates | [ ] |
| B-5 | Distributed tracing | [ ] |
| B-6 | GitHub Actions CI/CD (test + lint) | [ ] |

## Deliverables
| ID | Deliverable | Status |
|----|-------------|--------|
| D-1 | Repo with **clean commit history** | [~] |
| D-2 | README: setup, architecture overview, API examples | [~] |
| D-3 | `docker-compose up` one-command setup | [ ] |
| D-4 | Swagger / OpenAPI spec | [ ] |
| D-5 | Versioned DB migrations | [ ] |
| D-6 | Test suite runnable with a single command | [ ] |

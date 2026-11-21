# Notification System

An event-driven, multi-channel notification service written in Go. It accepts notification
requests over HTTP, processes them asynchronously through prioritized queue workers, and delivers
them via SMS, Email and Push providers with rate limiting, retries and real-time status tracking.

> **Status:** 🚧 Phase 8 done: notifications flow end to end (API → PostgreSQL → dispatcher → Kafka → priority-scheduled workers → Redis rate limiter → provider). Failures are classified and retried with exponential backoff and jitter, terminal failures are dead-lettered, and a per-channel circuit breaker holds the backlog during provider outages. Every role exports Prometheus metrics, and one OpenTelemetry trace follows each notification from the API request to the provider call. Notifications can be scheduled and rendered from templates, and every status change streams to WebSocket clients (try http://localhost:8080/ws-demo). Hardening follows in Phase 9. See [`docs/plan.md`](docs/plan.md).

## Features (target)
- **Notification API:** single and batch create (up to 1000), status by ID / batch ID, cancel, filtered + paginated listing
- **Processing engine:** async workers, high / normal / low priority queues, 100 msg/s per channel rate limit, content validation, idempotency
- **Delivery & retry:** exponential backoff with jitter, failure classification, dead-letter handling
- **Observability:** Prometheus metrics (queue depth, success/failure rates, latency), structured logs with correlation and trace IDs, health checks, OpenTelemetry traces in Jaeger
- **Bonus:** scheduled notifications, `{{variable}}` templates, WebSocket status updates, distributed tracing, CI/CD

## Quick start
Requirements: Docker with Compose v2. Go 1.26+ only for local development.

```bash
docker compose up --build        # or: make up
curl -i localhost:8080/healthz   # API liveness, through the nginx load balancer
curl localhost:8080/readyz       # API readiness: {"status":"ok","checks":{"postgres":"ok"}}
```

| Service | Role | Host port |
|---------|------|-----------|
| `lb` | nginx load balancer in front of `api` | 8080 |
| `api` | public HTTP API (`notifier api`) | (via `lb`) |
| `dispatcher` ×2 | PostgreSQL → Kafka relay + reaper + maintenance (`notifier dispatcher`) | (admin :9090 in-network) |
| `worker` | channel consumers + delivery (`notifier worker`) | (admin :9090 in-network) |
| `mockprovider` | local fake of webhook.site with fault injection | 8081 (127.0.0.1 only) |
| `postgres`, `kafka`, `redis` | infrastructure | 5432, 9092, 6379 (127.0.0.1 only) |
| `jaeger` | trace storage and UI ([ADR-0009](docs/adr/0009-observability.md)) | 16686 UI, 4318 OTLP (127.0.0.1 only) |
| `prometheus` | scrapes `/metrics` of every replica | 9091 UI (127.0.0.1 only) |
| `kafka-init` | one-shot job creating the [ADR-0002](docs/adr/0002-kafka-topology-and-consumer-model.md) topics | |
| `migrate` | one-shot job: schema migrations + daily partitions (`notifier migrate`); the roles start after it | |

Every role exposes `GET /healthz` (liveness, never checks dependencies), `GET /readyz`
(readiness, checks the role's dependencies; failures are reported as `unavailable` and the error
is logged, not returned) and `GET /metrics` (Prometheus; blocked at the public load balancer). Every response
carries `X-Correlation-ID` (taken from the request if valid, otherwise generated) and
`X-Instance-ID`. A W3C `traceparent` request header is continued by the API's trace.

Mock provider:
```bash
curl -XPOST localhost:8081/any-path -H 'Idempotency-Key: n-1' \
  -d '{"to":"+905551234567","channel":"sms","content":"hi"}'
# 202 {"messageId":"...","status":"accepted","timestamp":"..."}
curl localhost:8081/stats        # accepted/rejected per channel + rate in the last second
```

`make help` lists all developer targets (`make run-api`, `make infra`, `make logs`, ...).

## Architecture

![Architecture](docs/architecture/architecture.png)

> Editable source: [`docs/architecture/architecture.excalidraw`](docs/architecture/architecture.excalidraw) (open with [excalidraw.com](https://excalidraw.com)).

**PostgreSQL decides *when*, Kafka moves *what*, workers deliver at ≤ 100 msg/s per channel.**

1. **API** validates the request, enforces `Idempotency-Key`, inserts the notification
   (`status=pending`) and returns `202 Accepted`. It never publishes work to Kafka; the
   dispatcher does.
2. **Dispatcher** claims due rows (`FOR UPDATE SKIP LOCKED`, FIFO by `next_attempt_at`), publishes
   them to Kafka and marks them `queued`. The same mechanism handles new, scheduled and retrying
   notifications.
3. **Kafka** carries claim-check messages on 9 work topics (`notifications.{channel}.{priority}`),
   plus `notifications.dlq` and `notification.events`.
4. **Channel workers** (one consumer group per channel) buffer records per priority and pick the
   next one with a **capped-aging scheduler**. `high` is never overtaken, while `normal`/`low` age
   and get a guaranteed minimum share. Each send takes a token from the shared **Redis** rate
   limiter, passes a per-channel circuit breaker (Phase 6), and the outcome is written back with a
   guarded status transition.
5. **Status events:** every component publishes the rows it changed to `notification.events`
   after the commit (best effort, [ADR-0011](docs/adr/0011-status-events-and-websocket.md)), and
   every API instance fans them out to WebSocket subscribers.

### Processing engine (Phase 5)
- **Dispatcher** ([`internal/dispatcher`](internal/dispatcher)): the relay locks up to 500 due rows,
  publishes them with an idempotent `acks=all` producer, and marks them `queued` together with the
  Kafka partition and offset of each record. It wakes on `NOTIFY notifications_due` and polls every
  500 ms as a fallback (scheduled and retrying rows). Replicas share the work through
  `FOR UPDATE SKIP LOCKED`.
- **Reaper** (every 30 s, in every dispatcher): expired worker leases go to `retrying`, or to
  `failed` with a dead letter if it was the last attempt. A `queued`
  row whose offset is behind the committed offset of `workers.{channel}` was consumed without a
  claim, so it goes back to `pending` (after a 1-minute grace period). A burst backlog is never
  re-published, however long it waits. Rows queued for more than 24 h fall back to `pending`.
- **Worker** ([`internal/worker`](internal/worker)): one consumer group member per channel. Records
  are routed into one min-heap per priority (pausing a topic when its heap is full). For each send
  the worker waits for a free sender (20 per channel), then for a rate-limit slot, and only then
  picks the head with the highest *capped-aging* score. It claims the row (the claim waits on the
  dispatcher's row lock if the record arrived before its commit), calls the provider with the
  notification id as `Idempotency-Key`, and persists the outcome. Offsets are committed per
  partition up to the highest contiguous finished record.
- **Rate limiter** ([`internal/ratelimit`](internal/ratelimit)): a reserving GCRA in Redis. Each send
  atomically reserves the channel's next 10 ms slot, shared by all replicas. A `429` sets a
  fleet-wide cool-down. If Redis is down nothing is sent (fail closed).
- Measured on the compose stack (1000 `low` SMS, then 100 `high`): all 1100 sent in 11.1 s with one
  worker and 11.4 s with three (the floor at 100/s is 11.0 s). The provider never saw more than 100
  SMS in a second, and the 100 `high` were delivered within 1.4 s despite the backlog.
- **Delivery outcome** ([ADR-0005](docs/adr/0005-delivery-retry-and-failure-handling.md)):
  `2xx` is `sent`. Timeouts, connection errors, `5xx`, `408` and `425` are retried with exponential
  backoff and equal jitter (`RETRY_BASE_DELAY` doubling up to `RETRY_MAX_DELAY`). A `429` pauses the
  whole fleet and is retried after at least its `Retry-After` without consuming an attempt: the
  provider is healthy and only asks us to slow down. Any other `4xx` is permanent. A
  permanent failure, or a failure of the last of `MAX_ATTEMPTS` attempts, moves the notification to
  `failed` and publishes a dead letter to `notifications.dlq`.
- **Circuit breaker** (per channel and worker instance): opens after 5 retryable failures in a row,
  or more than 50% of at least 20 sends in a minute. While open the worker picks nothing: records
  stay in its buffers and in Kafka, fetching pauses, and no attempts are burned. After 30 s one
  probe is let through; its success closes the breaker. `4xx` and `429` answers do not count.

### Live status updates (Phase 8)
See [ADR-0011](docs/adr/0011-status-events-and-websocket.md) for the design.

- **Events:** the API, the dispatcher (relay and reaper) and the workers publish every row they
  change to `notification.events` right after the commit. Publishing never waits; a lost event
  only delays an update, and `notifier_status_events_total{result}` counts the failures.
- **Hub:** each API instance reads every partition from the end without a consumer group, so any
  instance can serve any client and the load balancer needs no sticky sessions.
- **Protocol:** a client opens `GET /v1/ws` and subscribes to notification ids and batch ids. It
  first gets the current state (a snapshot), then every change. Updates may arrive out of order,
  so clients keep the latest `at` per notification.
- **Demo page:** http://localhost:8080/ws-demo sends a notification and follows it live.

```bash
websocat ws://localhost:8080/v1/ws
{"type":"subscribe","batchIds":["<batchId>"]}
# {"type":"subscribed","notificationIds":[],"batchIds":["<batchId>"]}
# {"type":"batch","batch":{"id":"<batchId>","total":2,"statusCounts":{"pending":2},...}}
# {"type":"status","event":{"notificationId":"…","batchId":"<batchId>","status":"queued","attempts":0,"at":"…",...}}
# {"type":"status","event":{"notificationId":"…","status":"processing","attempts":1,...}}
# {"type":"status","event":{"notificationId":"…","status":"sent","attempts":1,...}}
```

Limits:
- 1000 subscriptions per connection, 100 ids per message, and 10 messages handled per second.
- Snapshot reads are batched, and at most 4 run at once per instance.
- A client that falls 256 events behind is closed with `1008`.
- 10 000 connections per instance; further ones are closed with `1013`. On shutdown, connections are closed with `1001`; the
client reconnects, subscribes again and gets fresh snapshots. Metrics: `notifier_ws_connections`,
`notifier_ws_messages_total{type}`, `notifier_ws_disconnects_total{reason}`.

### Observability (Phase 7)
See [ADR-0009](docs/adr/0009-observability.md) for the design.

- **Metrics** ([`internal/metrics`](internal/metrics)): every role serves its own registry on
  `/metrics`, and the compose Prometheus (http://localhost:9091) scrapes all replicas. Main series:

  | Question | Metric |
  |----------|--------|
  | How big is the backlog? | `notifier_notifications{status}`, `notifier_notifications_due`, `notifier_oldest_due_age_seconds` (PostgreSQL), `notifier_kafka_consumer_lag{group,topic}` (Kafka), `notifier_worker_buffered_records` |
  | Are deliveries succeeding? | `notifier_delivery_outcomes_total{channel,outcome}` (sent, retrying, failed, released, skipped, ...), `notifier_provider_requests_total{channel,class}`, `notifier_dead_letters_total` |
  | How fast? | `notifier_delivery_latency_seconds` (first due → sent, across retries), `notifier_queue_wait_seconds` (due → picked), `notifier_provider_request_duration_seconds`, `notifier_http_request_duration_seconds{route}` |
  | Is something degraded? | `notifier_circuit_breaker_state{channel}` (0 closed, 1 half-open, 2 open), `notifier_reaper_recovered_total{reason}`, `notifier_worker_db_retries_total`, `notifier_dispatch_errors_total`, `notifier_status_events_total{result}` (failed, dropped) |

  ```promql
  sum by (channel) (rate(notifier_provider_requests_total{class="delivered"}[1m]))          # sends/s
  sum by (channel) (rate(notifier_delivery_outcomes_total{outcome="failed"}[5m]))
    / sum by (channel) (rate(notifier_delivery_outcomes_total{outcome=~"sent|failed"}[5m])) # failure ratio
  histogram_quantile(0.95, sum by (le, channel) (rate(notifier_delivery_latency_seconds_bucket[5m])))
  max by (topic) (notifier_kafka_consumer_lag)                                               # worker backlog
  ```
  The queue depth gauges are sampled by every dispatcher every `QUEUE_METRICS_INTERVAL`, so take
  `max()` over instances.
- **Traces** ([`internal/telemetry`](internal/telemetry)): with `OTEL_EXPORTER_OTLP_ENDPOINT` set
  (compose: Jaeger, UI at http://localhost:16686), each notification gets one trace:
  `POST /v1/notifications` → `send notifications.sms.high` (dispatcher) → `process
  notifications.sms.high` (worker) → `claim`, `HTTP POST` (provider), `write outcome`. The API stores
  the request's `traceparent` with the row and the dispatcher puts its span in the Kafka headers,
  so retries hours later still join the same trace. Find a trace by the `correlation_id` tag.
- **Logs**: JSON lines carry `correlation_id` and, inside a recorded span, `trace_id` and
  `span_id`.
- Measured on the compose stack (3000 SMS in three batches of 1000): `notifier_provider_requests_total`
  ran at 99.8/s, `high` drained first (median queue wait 5.6 s against 18.5 s for `normal`), and
  `notifier_kafka_consumer_lag` showed the remaining `normal` and `low` backlog per topic.

### Design decisions
| ADR | Decision |
|-----|----------|
| [0001](docs/adr/0001-architecture-overview.md) | PostgreSQL as source of truth + Kafka as delivery backbone |
| [0002](docs/adr/0002-kafka-topology-and-consumer-model.md) | Channel × priority topics, classic consumer groups, `franz-go` |
| [0003](docs/adr/0003-dispatch-without-separate-outbox.md) | The notifications table is the outbox; FIFO dispatcher + reaper |
| [0004](docs/adr/0004-idempotency.md) | Idempotency-Key at the API, guarded claim at the worker, provider idempotency header |
| [0005](docs/adr/0005-delivery-retry-and-failure-handling.md) | Status lifecycle, failure classification, backoff + jitter, DLQ, circuit breaker |
| [0006](docs/adr/0006-rate-limiting-and-priority-scheduling.md) | Redis GCRA per channel/provider, 429 fleet cool-down, capped-aging priority scheduler |
| [0007](docs/adr/0007-technology-stack.md) | Go 1.26, chi, pgx, goose, franz-go, go-redis, OpenTelemetry, testcontainers |
| [0008](docs/adr/0008-scalability-and-deployment-topology.md) | Scaling model per component, single PG primary + HA/replica/partitioning, nginx scaling demo |
| [0009](docs/adr/0009-observability.md) | Prometheus metrics per role, queue depth sampling, `traceparent` stored with the row and carried in Kafka headers, trace ids in logs |
| [0010](docs/adr/0010-templates.md) | Stored templates, `{{name}}` substitution only, rendered and validated at creation |
| [0011](docs/adr/0011-status-events-and-websocket.md) | Best-effort status events after each commit; every API instance reads all events and serves WebSocket subscriptions by notification or batch id |

## Data model
Schema: [`migrations/`](migrations) (goose, forward-only, embedded in the binary and applied by
`notifier migrate`).

| Table | Purpose |
|-------|---------|
| `notifications` | One row per message: content, status, attempts, `next_attempt_at` (drives dispatch, scheduling and retries), lease, provider result, the creating request's `correlation_id` and `trace_parent` ([ADR-0009](docs/adr/0009-observability.md)), and the `template_id`/`template_version` its content was rendered from. Also the dispatch outbox ([ADR-0003](docs/adr/0003-dispatch-without-separate-outbox.md)) |
| `notification_status_history` | Append-only audit trail: one row per transition (from, to, attempt, reason) |
| `batches` | One row per batch request; status counts are aggregated from its notifications |
| `templates` | Message templates with `{{variable}}` placeholders; `version` goes up on every update ([ADR-0010](docs/adr/0010-templates.md)) |
| `idempotency_keys` | `Idempotency-Key` → request hash + created resource, with a TTL ([ADR-0004](docs/adr/0004-idempotency.md)) |

- **Daily partitions** of `notifications` and its history, created days ahead and dropped after
  `RETENTION_DAYS` by a maintenance loop in the dispatcher (advisory-locked, `DETACH ...
  CONCURRENTLY`). Dropping a partition replaces bulk `DELETE`s and vacuum storms.
- **Ids are UUIDv7 and `created_at` is the timestamp inside the id**, so a lookup by id alone is
  pruned to one partition and list pagination is a keyset on `id`. Batch items share the batch's
  timestamp and sort in request order.
- **Every status change is a guarded `UPDATE ... WHERE status = ANY(allowed)`** that appends the
  history row in the same statement and is checked against the state machine in
  [`internal/domain/status.go`](internal/domain/status.go). Outcomes reported by a worker are also
  fenced by the `lease_token` of its claim. Racing transitions (a Kafka redelivery, a late worker
  whose lease was reaped and the row claimed again, a cancel during dispatch) have exactly one
  winner, and each of these races has an integration test.
- Indexes: partial indexes on `next_attempt_at` (due work), `lease_until` (expired leases),
  `queued_at` (age fallback for `queued` rows) and `(channel, priority, kafka_partition,
  kafka_offset)` (queued rows behind the committed offset), `(status, id)` for filtered lists and
  `(batch_id, id)` for batches. Batches older than the retention are deleted by the same
  maintenance loop.

## Scalability & capacity

![Deployment](docs/architecture/deployment.png)

**The throughput ceiling is the provider quota:** 3 channels × 100 msg/s = 300 msg/s ≈ 26 M/day.
Adding replicas or database capacity cannot raise it, so scaling serves **burst absorption**
(API), **resilience** (every tier) and **provider-latency concurrency** (workers). The full
back-of-the-envelope math is in [`docs/capacity-planning.md`](docs/capacity-planning.md).

| At the ceiling | Value |
|----------------|-------|
| PostgreSQL writes | ~2.4 k/s (8 per notification), single primary is enough |
| PostgreSQL storage | ~2 KB/notification → ~52 GB/day, daily partitions + 30-day retention |
| Kafka | ~1.5 k msg/s (work + events), 3 brokers RF=3 |
| Redis | ~600 ops/s |
| Burst drain | 2 M SMS ≈ 5.6 h at 100 msg/s, so the backlog stays durable in Kafka and priority matters |

| Component | Scales by | Autoscaling signal |
|-----------|-----------|--------------------|
| API | horizontal behind LB (stateless) | CPU / RPS |
| Dispatcher | horizontal (`SKIP LOCKED`), 2 for HA | none |
| Workers | consumer group per channel | Kafka consumer lag (max = partitions) |
| PostgreSQL | vertical + sync standby + read replica + partitioning | none (no sharding needed) |

Scaling demo (local):
```bash
docker compose up -d --build --scale api=3 --scale worker=3     # or: make up-scaled
for i in $(seq 9); do curl -s -o /dev/null -D - localhost:8080/healthz | grep -i x-instance; done | sort | uniq -c
#   3 X-Instance-Id: 1dd48576fb42
#   3 X-Instance-Id: 9971cade7c41
#   3 X-Instance-Id: df695523475e
```
nginx (`lb`, port 8080) round-robins requests across API replicas and re-resolves the replica
set every 5 s, so `--scale` changes need no restart. Kafka splits partitions across worker
replicas, while each channel still sends at most 100 msg/s. See
[ADR-0008](docs/adr/0008-scalability-and-deployment-topology.md).

**Delivery guarantee:** at-least-once with deduplication at every hop. The only remaining
duplicate window is a worker crash after the provider accepted a request but before the outcome
was persisted (see ADR-0004).

## API
Contract: [`api/openapi.yaml`](api/openapi.yaml) (OpenAPI 3.1, written before the handlers and
linted in CI). With the stack running, browse it at **http://localhost:8080/docs** (Swagger UI).

| Method & path | Purpose | Success |
|---------------|---------|---------|
| `POST /v1/notifications` | Create one notification (`Idempotency-Key` supported) | `202` |
| `POST /v1/notifications/batch` | Create 1–1000 notifications, all-or-nothing validation | `202` |
| `GET /v1/notifications/{id}` | Status and details | `200` |
| `GET /v1/notifications/{id}/history` | Every status change with attempt and reason | `200` |
| `POST /v1/notifications/{id}/cancel` | Cancel while `pending`, `queued` or `retrying` | `200` |
| `POST /v1/notifications/{id}/retry` | Replay a `failed` notification | `202` |
| `GET /v1/notifications` | Filter by `status`, `channel`, `batchId`, `createdFrom`/`createdTo`; cursor pagination | `200` |
| `GET /v1/batches/{id}` | Batch with per-status counts | `200` |
| `POST /v1/templates` | Create a template with `{{variable}}` placeholders | `201` |
| `GET /v1/templates` | List templates, filter by `channel`; cursor pagination | `200` |
| `GET /v1/templates/{id}` | Template with its variables and version | `200` |
| `PUT /v1/templates/{id}` | Replace name and body (`version` + 1; channel fixed) | `200` |
| `DELETE /v1/templates/{id}` | Delete; notifications created from it are unaffected | `204` |
| `GET /v1/ws` | WebSocket: live status of notifications and batches | `101` |

```bash
# Create a high-priority OTP; repeating it with the same key returns the same notification
curl -i -XPOST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: otp-482913' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Your code is 482913","priority":"high"}'
# HTTP/1.1 202 Accepted
# Location: /v1/notifications/01a1fd17-4a0b-7869-8fa8-a36f9d5fb75a
# {"id":"01a1fd17-...","channel":"sms","priority":"high","status":"pending","attempts":0,...}

# Batch
curl -XPOST localhost:8080/v1/notifications/batch -H 'Content-Type: application/json' -d '{
  "notifications": [
    {"recipient":"user@example.com","channel":"email","content":"Flash sale!","priority":"low"},
    {"recipient":"fcm-token-123456","channel":"push","content":"Flash sale!","priority":"low",
     "idempotencyKey":"campaign-42-user-7"}]}'
curl localhost:8080/v1/batches/<batchId>                          # {"total":2,"statusCounts":{"pending":2},...}
curl 'localhost:8080/v1/notifications?batchId=<batchId>&limit=50'  # {"items":[...],"nextCursor":"..."}

# Validation errors list every violation
curl -XPOST localhost:8080/v1/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"0555","channel":"sms","content":""}'
# 422 {"error":{"code":"validation_failed","message":"request validation failed","details":[
#   {"field":"recipient","message":"must be an E.164 phone number, e.g. +905551234567"},
#   {"field":"content","message":"is required"}]}}
```

**Validation rules (FR-2.4):**

| Channel | Recipient | Content |
|---------|-----------|---------|
| `sms` | E.164 (`+905551234567`) | ≤ 10 segments: 1530 GSM-7 chars, or 670 if UCS-2 is needed (e.g. `ş`, `ı`, emoji) |
| `email` | bare address (`user@example.com`, ≤ 254 bytes) | ≤ 64 KiB |
| `push` | device token, 8–4096 printable ASCII chars | ≤ 4 KiB (APNs/FCM payload limit) |

**Scheduled delivery (B-2):** set `scheduledAt` to deliver later.

```bash
curl -XPOST localhost:8080/v1/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"user@example.com","channel":"email","content":"Your trial ends tomorrow",
       "scheduledAt":"2026-11-22T09:00:00+03:00"}'
# 202 {"status":"pending","scheduledAt":"2026-11-22T06:00:00Z","nextAttemptAt":"2026-11-22T06:00:00Z",...}
```

How it works:
- The notification is stored as `pending` with `next_attempt_at = scheduledAt`, so it needs no
  scheduler of its own.
- The dispatcher's fallback poll (`DISPATCH_POLL_INTERVAL`, 500 ms) publishes it once it is due;
  from then on it follows the normal priority and rate-limit path.
- It can be cancelled until a worker claims it.
- `scheduledAt` may be at most `MAX_SCHEDULE_AHEAD` (7 days) ahead, which keeps it inside the
  data retention.
- A time in the past means "as soon as possible", so idempotent retries keep working after the
  scheduled time has passed.

**Templates (B-3):** store the text once and send it with per-recipient variables
([ADR-0010](docs/adr/0010-templates.md)).

```bash
curl -XPOST localhost:8080/v1/templates -H 'Content-Type: application/json' \
  -d '{"name":"order_shipped","channel":"sms","body":"Hi {{name}}, order {{order_id}} has shipped."}'
# 201 {"id":"<templateId>","variables":["name","order_id"],"version":1,...}

curl -XPOST localhost:8080/v1/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","templateId":"<templateId>","variables":{"name":"Ayşe","order_id":"A-42"}}'
# 202 {"channel":"sms","content":"Hi Ayşe, order A-42 has shipped.","templateId":"<templateId>","templateVersion":1,...}
```

How templates behave:
- Placeholders are `{{ name }}` only: no logic, no functions.
- The text is rendered when the notification is created and must meet the channel's limits like
  any other content. A missing or unused variable is a `variables.<name>` violation.
- `channel` may be omitted and is taken from the template.
- Editing a template bumps its `version` and never changes notifications that were already
  accepted.
- Batch items can mix templates and plain content.
- The `Idempotency-Key` hash covers `templateId` and `variables`, so a retry after a template
  edit still replays the original notification.

Unknown JSON fields are rejected. Status codes: `400` malformed request, `404` unknown id, `409`
`invalid_state`, `idempotency_key_reused` (for batch items, `details` names the item) or
`already_exists` (template name), `413`
body over 256 KiB (10 MiB for batches), `415` non-JSON body, `422` validation, `503` timeout.
SMS segment counting follows carrier packing (an escape sequence or surrogate pair is never split
across segments).

## Configuration
All configuration comes from environment variables, parsed once at start-up. Invalid values
stop the process with a message listing every problem. Copy [`.env.example`](.env.example) to
`.env` to override defaults (docker-compose reads it automatically).

| Variable | Default | Used by | Description |
|----------|---------|---------|-------------|
| `LOG_LEVEL` | `info` | all | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | all | `json` or `text` |
| `INSTANCE_ID` | hostname | all | Replica id in logs and the `X-Instance-ID` header |
| `SHUTDOWN_TIMEOUT` | `15s` | all | Graceful shutdown deadline. Keep it below the compose `stop_grace_period` (20s), or Docker kills the process mid-drain |
| `HTTP_ADDR` | `:8080` | api | Public API listen address |
| `ADMIN_ADDR` | `:9090` | dispatcher, worker | Health and metrics listen address |
| `DATABASE_URL` | `postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable` | all | PostgreSQL connection string |
| `KAFKA_BROKERS` | `localhost:9092` | dispatcher, worker | Comma-separated seed brokers |
| `REDIS_ADDR` | `localhost:6379` | worker | Redis address |
| `REDIS_PASSWORD` | _(empty)_ | worker | Redis password |
| `IDEMPOTENCY_TTL` | `72h` | api | How long an `Idempotency-Key` is remembered. Must not exceed `RETENTION_DAYS` |
| `MAX_SCHEDULE_AHEAD` | `168h` | api | Latest allowed `scheduledAt`. Must be at least one day shorter than `RETENTION_DAYS` |
| `REQUEST_TIMEOUT` | `10s` | api | Deadline for each API request; exceeded requests return `503 timeout` |
| `RETENTION_DAYS` | `30` | dispatcher | Daily partitions older than this are dropped |
| `PARTITION_PREMAKE_DAYS` | `7` | migrate, dispatcher | Partitions kept ready ahead of today |
| `MAINTENANCE_INTERVAL` | `1h` | dispatcher | How often partition and key maintenance runs |
| `DISPATCH_BATCH_SIZE` | `500` | dispatcher | Due rows published per transaction |
| `DISPATCH_POLL_INTERVAL` | `500ms` | dispatcher | Fallback poll for scheduled and retrying rows; new rows wake the relay through `NOTIFY` |
| `REAPER_INTERVAL` | `30s` | dispatcher | How often expired leases and lost `queued` rows are recovered |
| `REAPER_GRACE` | `1m` | dispatcher | `queued` rows younger than this are never requeued |
| `REAPER_STUCK_AFTER` | `24h` | dispatcher | Age fallback for `queued` rows the offset rule cannot judge |
| `WORKER_CHANNELS` | `sms,email,push` | worker | Channels this process delivers (one consumer group member each) |
| `WORKER_CONCURRENCY` | `20` | worker | Sends in flight per channel |
| `PROVIDER_URL` | `http://localhost:8081/` | worker | Delivery provider endpoint, e.g. `https://webhook.site/<uuid>`. Compose defaults to the mock provider |
| `PROVIDER_TIMEOUT` | `10s` | worker | Timeout of each provider request |
| `LEASE_DURATION` | `30s` | worker | Claim lifetime; must be at least `PROVIDER_TIMEOUT` + 15s (up to 5s for the claim, 2s for a fresh rate-limit token, 5s reserved for writing the outcome). On shutdown a worker finishes in-flight sends, each bounded by the lease, so the worker's compose `stop_grace_period` (40s) must exceed it |
| `RATE_LIMIT_SMS` / `_EMAIL` / `_PUSH` | `100` | worker | Messages per second per channel, across all workers (FR-2.2) |
| `RATE_LIMIT_BURST` | `1` | worker | Sends allowed back to back after an idle period; above 1 a second can exceed the limit by `burst - 1` |
| `RETRY_BASE_DELAY` | `5s` | worker | Backoff ceiling of the first retry; it doubles per attempt and the delay is jittered between half the ceiling and the ceiling |
| `RETRY_MAX_DELAY` | `5m` | worker | Cap of the backoff ceiling; must not be shorter than `RETRY_BASE_DELAY` |
| `MAX_ATTEMPTS` | `5` | worker, dispatcher | Attempt budget; the last failure (or an expired lease on the last attempt) marks the notification `failed`. Set the same value on workers and dispatchers |
| `BREAKER_CONSECUTIVE_FAILURES` | `5` | worker | Retryable failures in a row that open a channel's circuit breaker |
| `BREAKER_FAILURE_RATIO` / `BREAKER_MIN_REQUESTS` / `BREAKER_WINDOW` | `0.5` / `20` / `1m` | worker | Also open it when more than this share of at least this many sends in the rolling window failed |
| `BREAKER_OPEN_TIMEOUT` | `30s` | worker | How long the breaker stays open before one probe; notifications released while open are retried after it |
| `SCHEDULER_AGING_RATE` | `2` | worker | Priority points gained per second waited (capped per priority, [ADR-0006](docs/adr/0006-rate-limiting-and-priority-scheduling.md)) |
| `SCHEDULER_MIN_SHARE` | `10` | worker | Every Nth send goes to `normal`/`low` under a `high` flood (0 disables) |
| `QUEUE_METRICS_INTERVAL` | `15s` | dispatcher | How often queue depth (PostgreSQL) and consumer lag (Kafka) are sampled for `/metrics` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(empty: tracing off)_ | all | Base URL of an OTLP/HTTP collector; spans go to `<url>/v1/traces`. Compose sets `http://jaeger:4318` unless `.env` sets it to empty |
| `TRACE_SAMPLE_RATIO` | `1` | all | Share of new traces recorded (0–1); spans with a parent follow the parent's decision |
| `MOCK_ADDR` | `:8081` | mockprovider | Listen address |
| `MOCK_LATENCY` / `MOCK_LATENCY_JITTER` | `50ms` / `100ms` | mockprovider | Simulated latency: base + uniform `[0, jitter)` |
| `MOCK_FAILURE_RATE` | `0` | mockprovider | Probability of HTTP 500 |
| `MOCK_RATE_LIMIT_RATE` | `0` | mockprovider | Probability of HTTP 429 with `Retry-After` |
| `MOCK_PERMANENT_RATE` | `0` | mockprovider | Probability of HTTP 400 (a permanent failure: not retried, dead-lettered) |
| `MOCK_RETRY_AFTER` | `1s` | mockprovider | `Retry-After` value for 429 |

## Testing
```bash
make test         # all tests with the race detector
make test-short   # unit tests only, no Docker needed
make lint         # golangci-lint (config: .golangci.yml)
make cover        # coverage summary
```
Integration tests start the compose images (PostgreSQL 17, Kafka 4.1, Redis 7) with
[testcontainers-go](https://golang.testcontainers.org/) (`internal/testinfra`). They need Docker
and are skipped by `make test-short`:
- `internal/storage`: concurrency (`SKIP LOCKED` dispatchers, racing claims, a claim racing the
  dispatch commit, concurrent idempotent creates), Kafka positions and the offset reaper rule,
  partition pruning and the partition lifecycle.
- `internal/dispatcher`, `internal/ratelimit`: publishing and committed offsets on Kafka, and the
  shared rate across several limiters on Redis.
- `internal/integration`: the engine end to end. Two workers share the group, every notification is
  delivered exactly once, a 503 is retried, a cancelled row is never sent, the rate holds, and a
  lost record is recovered by the reaper. A 400 fails at once, a provider stuck on 503 exhausts
  the attempts with growing backoff, both reach `notifications.dlq`, and during a provider outage
  the breaker stops the calls without burning attempts. A consumer of `notification.events`
  sees every transition of a delivered and of a retried notification, in order of `at`.
  A WebSocket client subscribed to a batch through a hub reading the real topic gets the
  snapshot, then a `sent` event for every item. One notification retried once is followed
  through a single trace: two dispatches under the stored request span, each delivery under its
  dispatch, and the `traceparent` reaching the provider on both calls.
  A scheduled notification reaches the provider at its `scheduledAt` (not before, and within the
  poll interval after it), one cancelled before its time is never sent, and one scheduled in the
  past goes out at once.

The WebSocket hub (`internal/ws`) is tested over real connections: snapshots, routing by
notification and batch id, protocol errors, limits, slow consumers, shutdown, the origin check
and binary frames.
The worker engine (scheduler, watermarks, backpressure, rebalance, shutdown, outcome policy,
circuit breaker, claim retry during a database outage) is unit-tested with fakes, without Kafka,
including the metrics it records and the span tree of a delivery.
CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs lint, race-enabled tests with
coverage and a Docker image build on every push to `main` and every pull request. No test calls
the real webhook.site; the provider is faked with `httptest` or the mock provider.

## Project docs
| Path | Contents |
|------|----------|
| [`docs/requirements.md`](docs/requirements.md) | Requirements & traceability checklist |
| [`docs/plan.md`](docs/plan.md) | Delivery plan & open questions |
| [`docs/conventions.md`](docs/conventions.md) | Engineering conventions |
| [`docs/adr/`](docs/adr/) | Architecture Decision Records |
| [`docs/architecture/`](docs/architecture/) | Architecture and deployment diagrams (Excalidraw source + PNG) |
| [`docs/capacity-planning.md`](docs/capacity-planning.md) | Back-of-the-envelope capacity math |
| [`.claude/agents/`](.claude/agents/) | Claude Code subagents used for parallel development |

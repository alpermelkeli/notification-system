# Configuration and services

## Services

The compose stack (`docker compose up --build`) runs these services:

| Service | Role | Host port |
|---------|------|-----------|
| `lb` | nginx load balancer in front of `api` | 8080 |
| `api` | public HTTP API (`notifier api`) | (via `lb`) |
| `dispatcher` ×2 | PostgreSQL → Kafka relay + reaper + maintenance (`notifier dispatcher`) | (admin :9090 in-network) |
| `worker` | channel consumers + delivery (`notifier worker`) | (admin :9090 in-network) |
| `mockprovider` | local fake of webhook.site with fault injection | 8081 (127.0.0.1 only) |
| `postgres`, `kafka`, `redis` | infrastructure | 5432, 9092, 6379 (127.0.0.1 only) |
| `jaeger` | trace storage and UI ([ADR-0009](adr/0009-observability.md)) | 16686 UI, 4318 OTLP (127.0.0.1 only) |
| `prometheus` | scrapes `/metrics` of every replica | 9091 UI (127.0.0.1 only) |
| `kafka-init` | one-shot job creating the [ADR-0002](adr/0002-kafka-topology-and-consumer-model.md) topics | |
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
curl -X PUT localhost:8081/admin/faults -d '{"failureRate":1,"rateLimitRate":0,"permanentRate":0}'  # change faults live
```

## Environment variables

All configuration comes from environment variables, parsed once at start-up. Invalid values
stop the process with a message listing every problem. Copy [`.env.example`](../.env.example) to
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
| `WS_ALLOWED_ORIGINS` | *(empty)* | api | Extra browser origins allowed to open `/v1/ws`, comma-separated host patterns (`app.example.com`, `*.example.com`) or with a scheme (`https://app.example.com`), matched against the origin's host and non-default port (`*.example.com` excludes `example.com` itself); the API's own origin is always allowed. Set it behind a proxy that rewrites `Host` |
| `DEMO_ENABLED` | `false` | api | Serve the playground at `/playground`. A local demo: anyone who can open it (compose publishes port 8080 on every interface) can create load and change the mock provider's faults. Compose sets `true`; `DEMO_ENABLED=false` in `.env` turns it off |
| `DEMO_PROMETHEUS_URL` | *(empty)* | api | Prometheus server the playground reads its live counters from; empty hides them. Compose: `http://prometheus:9090` |
| `DEMO_PROVIDER_ADMIN_URL` | *(empty)* | api | Mock provider base URL for the playground's failure scenarios; empty disables them. They are also off when `PROVIDER_URL` is on another host (for example webhook.site), because the faults would not affect delivery. The scenarios always leave the faults at 0. Compose: `http://mockprovider:8081` |
| `RETENTION_DAYS` | `30` | dispatcher | Daily partitions older than this are dropped |
| `PARTITION_PREMAKE_DAYS` | `7` | migrate, dispatcher | Partitions kept ready ahead of today |
| `MAINTENANCE_INTERVAL` | `1h` | dispatcher | How often partition and key maintenance runs |
| `DISPATCH_BATCH_SIZE` | `500` | dispatcher | Due rows published per transaction |
| `DISPATCH_POLL_INTERVAL` | `500ms` | dispatcher | Fallback poll for scheduled and retrying rows; new rows wake the relay through `NOTIFY` |
| `REAPER_INTERVAL` | `30s` | dispatcher | How often expired leases and lost `queued` rows are recovered |
| `REAPER_GRACE` | `1m` | dispatcher | `queued` rows younger than this are never requeued |
| `REAPER_STUCK_AFTER` | `24h` | dispatcher | Age fallback for `queued` rows the offset rule cannot judge |
| `WORKER_CHANNELS` | `sms,email,push` | worker | Channels this process delivers (one consumer group member each) |
| `WORKER_CONCURRENCY` | `32` | worker | Sends in flight per channel; size it as rate × (50 ms send offset + provider latency + claim) |
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
| `SCHEDULER_AGING_RATE` | `2` | worker | Priority points gained per second waited (capped per priority, [ADR-0006](adr/0006-rate-limiting-and-priority-scheduling.md)) |
| `SCHEDULER_MIN_SHARE` | `10` | worker | Every Nth send goes to `normal`/`low` under a `high` flood (0 disables) |
| `QUEUE_METRICS_INTERVAL` | `15s` | dispatcher | How often queue depth (PostgreSQL) and consumer lag (Kafka) are sampled for `/metrics`. Compose sets `5s`, the Prometheus scrape interval, so the playground's backlogs stay fresh |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(empty: tracing off)_ | all | Base URL of an OTLP/HTTP collector; spans go to `<url>/v1/traces`. Compose sets `http://jaeger:4318` unless `.env` sets it to empty |
| `TRACE_SAMPLE_RATIO` | `1` | all | Share of new traces recorded (0–1); spans with a parent follow the parent's decision |
| `MOCK_ADDR` | `:8081` | mockprovider | Listen address |
| `MOCK_LATENCY` / `MOCK_LATENCY_JITTER` | `50ms` / `100ms` | mockprovider | Simulated latency: base + uniform `[0, jitter)` |
| `MOCK_FAILURE_RATE` | `0` | mockprovider | Probability of HTTP 500 (initial value; `PUT /admin/faults` changes all three rates at runtime) |
| `MOCK_RATE_LIMIT_RATE` | `0` | mockprovider | Probability of HTTP 429 with `Retry-After` |
| `MOCK_PERMANENT_RATE` | `0` | mockprovider | Probability of HTTP 400 (a permanent failure: not retried, dead-lettered) |
| `MOCK_RETRY_AFTER` | `1s` | mockprovider | `Retry-After` value for 429 |

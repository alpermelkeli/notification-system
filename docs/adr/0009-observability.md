# 0009. Observability: metrics, trace propagation and log correlation

- **Status:** accepted
- **Date:** 2026-11-21

## Context
FR-4.1 asks for a real-time metrics endpoint covering queue depth, success and failure rates and
latency. FR-4.2 asks for structured logs with correlation IDs, and B-5 for distributed tracing.
ADR-0007 already picked `prometheus/client_golang` for metrics and OpenTelemetry with OTLP to
Jaeger for traces. This ADR records how they are applied.

Two properties of the design shape the work:

- A notification crosses two hops that are not HTTP. The API writes a row, the dispatcher
  publishes it to Kafka at some later time (now, at its scheduled time, or after a retry
  backoff), and a worker consumes it. A trace has to survive the row and the Kafka record.
- Queue depth lives in two places: due and unfinished rows in PostgreSQL, and uncommitted records
  in Kafka. Neither is visible from a single process.

## Options considered
1. **Trace context across the database hop**
   - *Store the creating request's `traceparent` with the row* (chosen). Every dispatch, every
     attempt and every provider call joins the API request's trace, even across retries and
     restarts. It costs one text column, added as a catalog-only change.
   - *Start a new trace in the dispatcher and link it to nothing.* The API request and the
     delivery could only be joined through the correlation ID, by hand.
   - *Span links instead of parent/child.* This is the semantically pure choice for messaging.
     Jaeger shows links far less usefully than one tree, though, and one notification's history
     is what operators want to see.
2. **Queue depth**
   - *Sample on an interval in the dispatcher* (chosen): one grouped count over the unfinished
     rows, plus `kadm.Lag` for the three consumer groups, every `QUEUE_METRICS_INTERVAL` (15 s).
     The cost stays fixed however often Prometheus scrapes.
   - *A collector that queries on every scrape.* Its database load grows with the number of
     scrapers and replicas.
   - *A separate Kafka exporter.* It is the standard in production, but adds another container
     and only covers Kafka.
3. **Metric registry**
   - *One registry per role, passed down explicitly* (chosen). There are no globals, every test
     gets a fresh registry, and each role only exports what it does.
   - *The default global registry.* It is simpler, but conflicts with the no-globals convention
     and leaks state between tests.
4. **HTTP server spans**
   - *Our own middleware* (chosen). otelhttp's handler names spans after `http.Request.Pattern`,
     which chi sets to the mount point (`/v1/*`) for sub-router routes. Our middleware names the
     span after the complete chi route once routing is done.
   - otelhttp is still used for the provider client (`otelhttp.NewTransport`), where it adds the
     client span and the `traceparent` header.

## Decision
**Metrics** (`internal/metrics`, served on `/metrics` by every role; blocked at the public LB):

| Area | Metrics |
|------|---------|
| API | `notifier_http_requests_total{method,route,status}`, `notifier_http_request_duration_seconds`, `notifier_http_requests_in_flight`, `notifier_notifications_created_total{channel,priority}` |
| Queue depth (dispatcher) | `notifier_notifications{status}` (pending, queued, processing, retrying), `notifier_notifications_due`, `notifier_oldest_due_age_seconds`, `notifier_kafka_consumer_lag{group,topic}`, `notifier_queue_depth_errors_total{source}` |
| Dispatch | `notifier_dispatched_total{channel,priority}`, `notifier_dispatch_errors_total`, `notifier_reaper_recovered_total{reason}` |
| Delivery (worker) | `notifier_delivery_outcomes_total{channel,outcome}`, `notifier_provider_requests_total{channel,class}`, `notifier_provider_request_duration_seconds`, `notifier_delivery_latency_seconds{channel,priority}` (from first due to sent, across retries), `notifier_queue_wait_seconds{channel,priority}` (from due to pick) |
| Worker state | `notifier_worker_sends_in_flight`, `notifier_worker_buffered_records{channel,priority}`, `notifier_circuit_breaker_state{channel}` (0/1/2), `notifier_circuit_breaker_transitions_total`, `notifier_worker_db_retries_total{channel,operation}` |
| Failures | `notifier_dead_letters_total{source,result}` |

Route labels use the chi pattern (`/v1/notifications/{id}`), never the raw path. Every
dispatcher replica reports the same depth values, so queries take `max()` over instances.

**Traces** (`internal/telemetry`): `main` installs the W3C propagator, plus an OTLP/HTTP
exporter with a batching processor when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Sampling is
parent-based with `TRACE_SAMPLE_RATIO` for new traces. Packages take tracers from the installed
global provider, as OpenTelemetry instrumentation does. The span tree of one delivery attempt:

```
POST /v1/notifications                (api, server)
└─ send notifications.sms.high        (dispatcher, producer; parent = traceparent stored in the row)
   └─ process notifications.sms.high  (worker, consumer; parent = traceparent in the Kafka headers)
      ├─ claim
      ├─ HTTP POST                    (provider client; sends traceparent to the provider)
      └─ write outcome
```

A retry adds another `send` / `process` pair under the same request span. An unsampled request
still stores its `traceparent`: its sampled flag tells the dispatcher and the worker not to record
either, so a trace is never recorded in part. Spans carry `notification.id`, `channel`,
`priority`, `attempt`, `correlation_id` and the outcome.

**Logs**: the slog handler adds `trace_id` and `span_id` next to `correlation_id` whenever the
context holds a recorded span, so a log line leads to its trace and back.

## Consequences
- ✅ Every requirement of FR-4.1 can be read from Prometheus: backlog per stage, rates by
  outcome, and latency at three levels (API request, provider call, end to end).
- ✅ One trace per notification shows where its time went, retries included.
- ✅ Tracing is optional. Without an endpoint the provider stays no-op, spans cost nothing, and no
  `traceparent` is stored. An unreachable collector only drops spans.
- ⚠️ The depth query counts unfinished rows. During a multi-million-row burst it scans the status
  index range of the backlog every 15 s per dispatcher replica. At that scale, sample from one
  replica (behind the maintenance advisory lock) or switch to estimates.
- ⚠️ `notifier_kafka_consumer_lag` covers partitions that have a committed offset or an assigned
  member. A brand-new group with no member reports nothing until a worker joins.
- ⚠️ Long-lived traces: a notification scheduled days ahead joins a request span that ended days
  earlier. Jaeger handles this, but in-memory Jaeger (compose) forgets old traces.
- Compose adds Jaeger (UI `localhost:16686`) and Prometheus (UI `localhost:9091`, which discovers
  scaled replicas through DNS). Grafana dashboards and alert rules are left for production.

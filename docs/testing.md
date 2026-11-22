# Testing

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
## Load test

```bash
make up          # or: make up-scaled
make loadtest    # go run ./cmd/loadtest -h lists the flags (size, concurrency, limit, ...)
```
[`cmd/loadtest`](../cmd/loadtest) drives the running stack through the public API and checks it
against the [capacity plan](capacity-planning.md). It refuses to start unless the mock provider
answers with no faults, so it never sends to webhook.site. In order:
1. 30 `high` notifications, 10 per second, with nothing else in the system: created-to-sent
   latency.
2. A burst of 30 000 `low` notifications in batches of 1000, 4 requests at a time, spread over
   the three channels: how fast the API accepts them, and when the dispatcher has published them.
3. 30 `high` SMS, 2 per second, right behind the burst: the same latency, split into the wait for
   the dispatcher and queued to sent.
4. The burst drains. Twice a second the test reads the requests the mock provider received in the
   last wall-clock second, per channel, and at the end it lists every notification for the drain
   rate.

It prints PASS or FAIL per check and exits non-zero on a failure. A second at the provider one
request (1%) above the limit is a WARN: tokens are exactly 10 ms apart, and a few milliseconds
of jitter on the HTTP path can move a send into the neighbouring second. Measured results are in
the [capacity plan](capacity-planning.md#measured).

## CI

CI ([`.github/workflows/ci.yml`](../.github/workflows/ci.yml)) runs lint, race-enabled tests with
coverage and a Docker image build on every push to `main` and every pull request. No test calls
the real webhook.site; the provider is faked with `httptest` or the mock provider.

# 0011. Status events and WebSocket updates

- **Status:** accepted
- **Date:** 2026-11-21

## Context
B-4 asks for real-time status updates over WebSocket. Earlier decisions already describe the
shape:

- ADR-0001 and ADR-0002 reserve the `notification.events` topic for status changes, keyed by
  notification id.
- ADR-0003 says events are published best-effort after the commit, by the component that changed
  the state, with PostgreSQL remaining the truth.
- ADR-0008 says any API instance can serve any WebSocket client, so the load balancer needs no
  sticky sessions.

Nothing publishes events yet, and the API has no WebSocket endpoint. Three components change the
state:

- the API (`created`, `cancelled`, and `pending` again on replay);
- the dispatcher (`queued`, plus the reaper's recoveries);
- the worker (`processing`, `sent`, `retrying`, `failed`).

## Options considered
1. **How events are produced**
   - *Each component publishes the rows it changed, after the commit, without waiting* (chosen,
     as ADR-0003 says). The storage methods return the changed rows; the bulk reaper statements
     now return rows instead of counts. A record that cannot be published is counted and
     dropped. The request or the delivery never waits for it.
   - *A transactional outbox or CDC.* It is reliable, but it costs a table write per transition
     or a Debezium deployment. No consumer here needs guaranteed delivery, because a client can
     always read the API.
   - *PostgreSQL `LISTEN/NOTIFY` straight to the API.* No Kafka is involved, but every API
     instance would hold a listening connection and receive every event. Payloads are limited
     to 8 kB, and nothing else could consume the stream.
2. **How API instances consume**
   - *No consumer group: each instance reads every partition from the end* (chosen). Every
     instance sees every event, and nothing is left on the broker when instances come and go.
     This amends ADR-0002, which proposed a unique group per instance: under autoscaling that
     would leave an orphaned group, with committed offsets, behind every pod that ever ran.
   - *A shared group.* Each event would reach only one instance, so a client's events would
     have to be routed to the instance holding its socket, and that needs sticky sessions or a
     second hop.
3. **What a client subscribes to**
   - *Notification ids and batch ids, sent as messages on an open socket* (chosen). A client
     can follow a batch it just created and add or drop single notifications without
     reconnecting.
   - *Filters fixed in the connect URL.* This is simpler, but every change needs a new
     connection.
   - *A firehose of all events.* It is not offered. With no authentication in the API, it would
     expose every recipient's delivery activity to anyone who connects.
4. **Library**
   - *`github.com/coder/websocket`* (chosen). It is the maintained successor of
     `nhooyr.io/websocket`, has no dependencies, and its API takes a `context.Context`. It works
     with `net/http` handlers and chi, and allows concurrent writes.
   - *`gorilla/websocket`.* It is more widely used, but it takes no context, and concurrent
     writes must be serialised by hand.

## Decision
**Event** (JSON on `notification.events`, keyed by notification id, with headers
`correlation_id` and `traceparent`):

```json
{"notificationId": "…", "batchId": "…", "channel": "sms", "priority": "high",
 "status": "retrying", "attempts": 2, "nextAttemptAt": "…", "lastError": "server_error: …",
 "at": "2026-11-21T15:04:05.123456Z"}
```

`at` is the row's `updated_at`, set with `clock_timestamp()` when the row changes, not with the
transaction's start time; the row lock orders one notification's changes, so their `at` follows
the same order (on one primary whose clock does not step back). The history row of the change
gets the same time, so `GET /v1/notifications/{id}/history` and the events agree. Events never carry the
recipient or the content. `lastError` is cut after the provider's status code, because the
response body that follows may echo the recipient; the REST API shows the full error.

**Publishing:** `internal/events.Publisher` uses franz-go's `TryProduce`.
- When the client's buffer is full, the record is dropped instead of blocking. A record that is
  never acknowledged within the delivery timeout is also dropped.
- Both cases are counted in `notifier_status_events_total{result}`.
- Every role publishes events on a Kafka client of their own: leader acks, a 2000-record buffer
  and a 3 s delivery timeout. On a shared client, event records waiting out a Kafka hiccup would
  fill the buffer that the dispatcher's work records and the workers' dead letters block on.
- The API's event client is not part of its readiness check: with Kafka down, creates and
  cancels still work and only the live updates pause.

**WebSocket** (`GET /v1/ws`, served by every API instance):
- **Client → server:** `{"type":"subscribe","notificationIds":[…],"batchIds":[…]}` and
  `{"type":"unsubscribe",…}`.
- **Server → client:**
  - `subscribed`, which acknowledges the full current set;
  - `status`, with the event above;
  - `error`, with a code and a message.
- **Snapshot:** after subscribing, the server sends the current state of each id it was given:
  a `status` built from the row for a notification, and a `batch` message with `statusCounts`
  for a batch. The subscription is registered before the snapshot is read, so no change between
  the two is lost. A client keeps, for each notification, the status with the latest `at`.
- **Limits:**
  - 1000 subscriptions per connection; a batch counts as one;
  - at most 100 ids per subscribe message;
  - client messages up to 64 KiB, handled at most 10 per second and connection; 8 may wait, and
    further ones are answered with `rate_limited`;
  - snapshots of new notification ids are read with one query, and at most 4 snapshot reads run
    at once per instance, so subscribers cannot take the database pool from the REST API;
  - 256 events may be queued per connection, and a client that falls further behind is
    disconnected with close code 1008; answers to the client's own requests have a separate
    queue, so they never cause that;
  - 10 000 connections per instance, beyond which connections are closed with 1013;
  - a ping every 30 s, from its own goroutine, detects dead peers. The reader only reads, so
    pongs are processed even while a request is handled.
- **Origin:** browsers must connect from the API's own origin, which is the library's default.
  The demo page at `/ws-demo` is served from it. Non-browser clients send no `Origin` header.
- **Shutdown:** on shutdown, open sockets are closed with 1001 (going away). Clients reconnect
  through the load balancer to another instance and subscribe again; the snapshot fills the gap.
- **Hub:** each API instance runs one hub, which routes an event to the subscribers of its
  notification id and its batch id. The `/v1/ws` route bypasses the request deadline, tracing
  and HTTP metrics middleware; it has its own metrics: `notifier_ws_connections`,
  `notifier_ws_messages_total{type}`, `notifier_ws_disconnects_total{reason}` and
  `notifier_ws_events_received_total`.

## Consequences
- ✅ Any instance serves any client. Scaling the API needs no sticky sessions, and losing an
  instance costs its clients only a reconnect.
- ✅ The delivery path is unchanged: publishing events cannot slow or fail a send, a claim or a
  create.
- ⚠️ Events are at most once and may arrive out of order: they come from different processes,
  so `processing` can arrive before `queued`. Clients order a notification's events by `at`,
  which follows the order of its changes, and the API stays the source of truth.
- ⚠️ Every API instance reads the whole event stream. At millions of events per hour, that is
  about 1–2 MB/s per instance, which is fine. Much larger fleets would partition the clients or
  put a fan-out tier, such as Redis pub/sub keyed by id, between Kafka and the sockets.
- ⚠️ There is no authentication, as in the rest of the API. Unguessable UUIDv7 ids (74 random
  bits) are the only guard. In production the socket would sit behind the same authentication
  and tenant checks as the REST API.
- `coder/websocket` v1.8.15 is added under ADR-0007.

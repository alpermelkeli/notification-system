# API guide

Contract: [`api/openapi.yaml`](../api/openapi.yaml) (OpenAPI 3.1, written before the handlers and
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
([ADR-0010](adr/0010-templates.md)).

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

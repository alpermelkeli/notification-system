# 0010. Templates with variable substitution

- **Status:** accepted
- **Date:** 2026-11-21

## Context
B-3 asks for a template system with variable substitution. Internal teams send the same message
shape to many recipients ("Your order {{order_id}} has shipped"). They want to keep the text in
one place and fill in the variables per notification.

Constraints from earlier decisions:

- Content is validated per channel at the API (FR-2.4). An SMS that needs 11 segments must be
  rejected with 422 when it is submitted, not fail at the provider hours later.
- The worker loads the content in its claim and sends it as it is (ADR-0002 claim-check). It
  should not need to know about templates.
- An idempotent retry of the same request must replay the original notification (ADR-0004).

## Options considered
1. **Where templates live**
   - *Stored templates* (chosen): a `templates` table with CRUD under `/v1/templates`. A
     notification names a `templateId` and passes `variables`.
   - *Inline templates only*: the request carries the template text and the variables. This
     saves no work for the caller, since they could render the text themselves, so it hardly
     counts as a template system.
2. **When a template is rendered**
   - *At creation, in the API* (chosen). The rendered text is validated against the channel's
     limits and stored in `content`, just as if the client had sent it. The row also records
     `template_id` and `template_version` for audit. Workers, retries and replays are unchanged,
     and editing a template never changes a notification that was already accepted.
   - *At send time, in the worker.* A template edit would change pending and scheduled
     notifications, which can be useful. But the content could no longer be validated when it
     is submitted, a failure would show up only as a failed delivery, and every retry would have
     to render again.
3. **Syntax**
   - *`{{name}}` substitution only* (chosen). There is no logic, no functions and no escaping
     rules, so a template can be checked completely when it is saved. Every placeholder is
     listed, and a malformed `{{` is rejected with its position.
   - *Go `text/template`.* It adds conditionals and loops, but its syntax is specific to Go, its
     errors are hard to explain to API clients, and it is more than notification copy needs.

## Decision
- **Template:** `id` (UUIDv7), unique `name` (`[a-z0-9][a-z0-9_.-]{0,99}`), `channel`, `body`,
  `version` (starts at 1 and goes up on every update), `created_at` and `updated_at`.
  - Placeholders are `{{ name }}`, where the name matches `[A-Za-z_][A-Za-z0-9_]{0,63}`; spaces
    inside the braces are allowed.
  - A body has at most 50 distinct variables.
  - The body must pass the channel's content rules as written. This catches an oversized body
    early; the rendered text is checked again.
  - The channel cannot change, because the body was validated for it.
  - Updates replace the name and body. Old versions are not kept, because no notification needs
    them: each one stores its rendered text.
  - A delete leaves notifications untouched; their `template_id` then points to nothing.
- **Notification request:** either `content` or `templateId` with `variables` (an object of
  strings), never both.
  - `channel` may be omitted and is then taken from the template. If it is given, it must match.
  - A missing or unused variable is a 422 violation that names the variable.
  - If the rendered text breaks the channel's limits, the violation is reported on `content`.
  - In a batch, every template is loaded with one query.
- **Idempotency:** the request hash covers `templateId` and `variables` instead of the rendered
  text. A retry after the template was edited still replays the original notification instead
  of failing with 409. The new fields are omitted when empty, so hashes of plain-content requests
  are unchanged.
- **Schema:** migration `00004` creates `templates` and adds the nullable `template_id` and
  `template_version` columns to `notifications`. Both are catalog-only changes on the
  partitioned table.

## Consequences
- ✅ Templates touch only the API and the storage layer. The engine, its retries and its
  guarantees are unchanged.
- ✅ The response shows the rendered content and which template version produced it.
- ⚠️ Editing a template does not update notifications that were already accepted, including
  scheduled ones. To change those, cancel them and create them again.
- ⚠️ Concurrent edits are last-write-wins. `version` shows that a change happened, but updates do
  not take `If-Match` yet.
- ⚠️ A literal `{{` cannot appear in a template body. Notification copy has not needed one; an
  escape sequence can be added later without breaking existing templates.
- At larger scale, templates are read on every templated request. Each read is a primary-key
  lookup that can be cached in memory per API instance, keyed by `id` and `version`.

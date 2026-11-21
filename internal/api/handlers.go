package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/httpx"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// createNotification handles POST /v1/notifications (FR-1.1, FR-2.4, FR-2.5).
func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if rerr := decodeJSON(w, r, maxSingleBody, &req); rerr != nil {
		h.writeError(w, r, rerr)
		return
	}
	key, rerr := idempotencyKey(r)
	if rerr != nil {
		h.writeError(w, r, rerr)
		return
	}
	templates, err := h.loadTemplates(r.Context(), req)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	in, canon, vs := h.validateCreate("", req, h.opts.Now(), templates)
	if len(vs) > 0 {
		h.writeError(w, r, validationFailed(vs))
		return
	}
	if key != nil {
		in.IdempotencyKey = &storage.IdempotencyKey{Key: *key, RequestHash: requestHash(canon)}
	}

	n, created, err := h.store.CreateNotification(r.Context(), in, origin(r.Context()))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if created {
		h.opts.Metrics.Created(n.Channel, n.Priority, 1)
		h.opts.Events.Publish(r.Context(), n)
	}
	w.Header().Set("Location", "/v1/notifications/"+n.ID.String())
	if !created {
		w.Header().Set(headerIdempotentReplay, "true")
	}
	httpx.WriteJSON(w, http.StatusAccepted, toNotificationJSON(n))
}

// createBatch handles POST /v1/notifications/batch (FR-1.2). The batch is all-or-nothing: every
// item is validated before anything is stored.
func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if rerr := decodeJSON(w, r, maxBatchBody, &req); rerr != nil {
		h.writeError(w, r, rerr)
		return
	}
	key, rerr := idempotencyKey(r)
	if rerr != nil {
		h.writeError(w, r, rerr)
		return
	}
	if n := len(req.Notifications); n == 0 || n > storage.MaxBatchSize {
		h.writeError(w, r, validationFailed([]violation{{Field: "notifications",
			Message: fmt.Sprintf("must contain 1 to %d items, got %d", storage.MaxBatchSize, n)}}))
		return
	}

	reqs := make([]createRequest, len(req.Notifications))
	for i, item := range req.Notifications {
		reqs[i] = item.createRequest
	}
	templates, err := h.loadTemplates(r.Context(), reqs...)
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	now := h.opts.Now()
	in := storage.NewBatch{Items: make([]storage.NewNotification, len(req.Notifications))}
	canon := make([]canonical, len(req.Notifications))
	seenKeys := make(map[string]int)
	var vs []violation
	for i, item := range req.Notifications {
		prefix := fmt.Sprintf("notifications[%d].", i)
		n, c, itemVs := h.validateCreate(prefix, item.createRequest, now, templates)
		vs = append(vs, itemVs...)
		if item.IdempotencyKey != nil {
			k := *item.IdempotencyKey
			switch j, dup := seenKeys[k]; {
			case !validKey(k):
				vs = append(vs, violation{Field: prefix + "idempotencyKey",
					Message: fmt.Sprintf("must be 1 to %d printable ASCII characters", maxIdempotencyKeyLength)})
			case dup:
				vs = append(vs, violation{Field: prefix + "idempotencyKey",
					Message: fmt.Sprintf("duplicates the key of notifications[%d]", j)})
			case key != nil && k == *key:
				vs = append(vs, violation{Field: prefix + "idempotencyKey", Message: "must differ from the Idempotency-Key header"})
			default:
				seenKeys[k] = i
				n.IdempotencyKey = &storage.IdempotencyKey{Key: k, RequestHash: requestHash(c)}
			}
		}
		in.Items[i] = n
		canon[i] = c
		if item.IdempotencyKey != nil {
			canon[i].Key = *item.IdempotencyKey
		}
	}
	if len(vs) > 0 {
		h.writeError(w, r, validationFailed(vs))
		return
	}
	if key != nil {
		in.IdempotencyKey = &storage.IdempotencyKey{Key: *key, RequestHash: requestHash(canon)}
	}

	res, err := h.store.CreateBatch(r.Context(), in, origin(r.Context()))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if res.Created {
		for _, it := range res.Items {
			if !it.Duplicate {
				h.opts.Metrics.Created(it.Notification.Channel, it.Notification.Priority, 1)
				h.opts.Events.Publish(r.Context(), it.Notification)
			}
		}
	}
	w.Header().Set("Location", "/v1/batches/"+res.Batch.ID.String())
	if !res.Created {
		w.Header().Set(headerIdempotentReplay, "true")
	}
	httpx.WriteJSON(w, http.StatusAccepted, toBatchCreatedJSON(res))
}

// getNotification handles GET /v1/notifications/{id} (FR-1.3).
func (h *Handler) getNotification(w http.ResponseWriter, r *http.Request) {
	h.withID(w, r, func(ctx context.Context, id uuid.UUID) (any, error) {
		n, err := h.store.GetNotification(ctx, id)
		return toNotificationJSON(n), err
	}, http.StatusOK)
}

// getHistory handles GET /v1/notifications/{id}/history.
func (h *Handler) getHistory(w http.ResponseWriter, r *http.Request) {
	h.withID(w, r, func(ctx context.Context, id uuid.UUID) (any, error) {
		changes, err := h.store.History(ctx, id)
		return toHistoryJSON(id, changes), err
	}, http.StatusOK)
}

// cancelNotification handles POST /v1/notifications/{id}/cancel (FR-1.4).
func (h *Handler) cancelNotification(w http.ResponseWriter, r *http.Request) {
	h.withID(w, r, func(ctx context.Context, id uuid.UUID) (any, error) {
		n, err := h.store.Cancel(ctx, id)
		if err == nil {
			h.opts.Events.Publish(ctx, n)
		}
		return toNotificationJSON(n), err
	}, http.StatusOK)
}

// retryNotification handles POST /v1/notifications/{id}/retry (B-1 replay).
func (h *Handler) retryNotification(w http.ResponseWriter, r *http.Request) {
	h.withID(w, r, func(ctx context.Context, id uuid.UUID) (any, error) {
		n, err := h.store.Replay(ctx, id)
		if err == nil {
			h.opts.Events.Publish(ctx, n)
		}
		return toNotificationJSON(n), err
	}, http.StatusAccepted)
}

// getBatch handles GET /v1/batches/{id} (FR-1.3).
func (h *Handler) getBatch(w http.ResponseWriter, r *http.Request) {
	h.withID(w, r, func(ctx context.Context, id uuid.UUID) (any, error) {
		b, err := h.store.GetBatch(ctx, id)
		return toBatchJSON(b), err
	}, http.StatusOK)
}

// withID parses the {id} path parameter, runs fn and writes its result with status. An id that
// is not a UUID cannot exist, so it is reported as not found.
func (h *Handler) withID(w http.ResponseWriter, r *http.Request, fn func(context.Context, uuid.UUID) (any, error), status int) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		h.writeError(w, r, domain.ErrNotFound)
		return
	}
	body, err := fn(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	httpx.WriteJSON(w, status, body)
}

// listNotifications handles GET /v1/notifications (FR-1.5).
func (h *Handler) listNotifications(w http.ResponseWriter, r *http.Request) {
	f, rerr := parseListFilter(r)
	if rerr != nil {
		h.writeError(w, r, rerr)
		return
	}
	page, err := h.store.ListNotifications(r.Context(), f)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	out := listJSON{Items: make([]notificationJSON, len(page.Items))}
	for i, n := range page.Items {
		out.Items[i] = toNotificationJSON(n)
	}
	if page.Next != nil {
		out.NextCursor = encodeCursor(*page.Next)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

func parseListFilter(r *http.Request) (storage.ListFilter, *requestError) {
	q := r.URL.Query()
	var f storage.ListFilter
	if v := q.Get("status"); v != "" {
		s, err := domain.ParseStatus(v)
		if err != nil {
			return f, badRequest("status: unknown value %q", v)
		}
		f.Status = &s
	}
	if v := q.Get("channel"); v != "" {
		c, err := domain.ParseChannel(v)
		if err != nil {
			return f, badRequest("channel: unknown value %q", v)
		}
		f.Channel = &c
	}
	if v := q.Get("batchId"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			return f, badRequest("batchId: not a UUID")
		}
		f.BatchID = &id
	}
	for _, p := range []struct {
		name string
		dst  **time.Time
	}{{"createdFrom", &f.CreatedFrom}, {"createdTo", &f.CreatedTo}} {
		if v := q.Get(p.name); v != "" {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return f, badRequest("%s: not an RFC 3339 timestamp", p.name)
			}
			*p.dst = &t
		}
	}
	if f.CreatedFrom != nil && f.CreatedTo != nil && !f.CreatedFrom.Before(*f.CreatedTo) {
		return f, badRequest("createdFrom must be before createdTo")
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > storage.MaxListLimit {
			return f, badRequest("limit: must be an integer from 1 to %d", storage.MaxListLimit)
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		id, ok := decodeCursor(v)
		if !ok {
			return f, badRequest("cursor: invalid")
		}
		f.After = &id
	}
	return f, nil
}

func validationFailed(vs []violation) *requestError {
	return &requestError{status: http.StatusUnprocessableEntity, code: "validation_failed",
		message: "request validation failed", violations: vs}
}

// writeError maps an error to the error envelope. Only this function knows HTTP status codes
// for domain errors (docs/conventions.md).
func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		rerr *requestError
		te   *domain.TransitionError
		ie   *storage.ItemError
	)
	switch {
	case errors.As(err, &rerr):
		details := make([]any, len(rerr.violations))
		for i, v := range rerr.violations {
			details[i] = v
		}
		httpx.WriteError(w, rerr.status, rerr.code, rerr.message, details...)
	case errors.Is(err, domain.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.As(err, &te):
		httpx.WriteError(w, http.StatusConflict, "invalid_state",
			fmt.Sprintf("notification is %s; this operation needs one of: %s", te.Current, allowedFor(te.Target)),
			map[string]string{"currentStatus": string(te.Current)})
	case errors.As(err, &ie) && errors.Is(err, domain.ErrIdempotencyKeyReused):
		httpx.WriteError(w, http.StatusConflict, "idempotency_key_reused", "an item idempotency key was already used with different content",
			violation{Field: fmt.Sprintf("notifications[%d].idempotencyKey", ie.Index), Message: "already used with different content"})
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		httpx.WriteError(w, http.StatusConflict, "idempotency_key_reused", err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		httpx.WriteError(w, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		h.logger.WarnContext(r.Context(), "request timed out", "method", r.Method, "path", r.URL.Path, "err", err)
		httpx.WriteError(w, http.StatusServiceUnavailable, "timeout", "the request timed out; it is safe to retry with the same Idempotency-Key")
	case errors.Is(err, domain.ErrInvalidArgument):
		httpx.WriteError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
	case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
		// The client went away; there is nobody to answer.
		h.logger.DebugContext(r.Context(), "request cancelled by client", "err", err)
	default:
		h.logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		httpx.WriteError(w, http.StatusInternalServerError, "internal_error",
			"internal error; quote the X-Correlation-ID when reporting it")
	}
}

// allowedFor names the source statuses an API operation targeting to accepts.
func allowedFor(to domain.Status) string {
	var from []domain.Status
	switch to {
	case domain.StatusCancelled:
		from = domain.CancellableStatuses
	case domain.StatusPending:
		from = []domain.Status{domain.StatusFailed}
	default:
		return "a different status"
	}
	s := ""
	for i, st := range from {
		if i > 0 {
			s += ", "
		}
		s += string(st)
	}
	return s
}

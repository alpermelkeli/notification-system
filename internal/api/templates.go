package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/httpx"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// createTemplate handles POST /v1/templates (B-3).
func (h *Handler) createTemplate(w http.ResponseWriter, r *http.Request) {
	in, rerr := h.decodeTemplate(w, r)
	if rerr != nil {
		h.writeError(w, r, rerr)
		return
	}
	t, err := h.store.CreateTemplate(r.Context(), in)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/templates/"+t.ID.String())
	httpx.WriteJSON(w, http.StatusCreated, toTemplateJSON(t))
}

// getTemplate handles GET /v1/templates/{id}.
func (h *Handler) getTemplate(w http.ResponseWriter, r *http.Request) {
	h.withID(w, r, func(ctx context.Context, id uuid.UUID) (any, error) {
		t, err := h.store.GetTemplate(ctx, id)
		return toTemplateJSON(t), err
	}, http.StatusOK)
}

// updateTemplate handles PUT /v1/templates/{id}: it replaces the name and body and increments the
// version. Notifications already created from the template keep their content (ADR-0010).
func (h *Handler) updateTemplate(w http.ResponseWriter, r *http.Request) {
	in, rerr := h.decodeTemplate(w, r)
	if rerr != nil {
		h.writeError(w, r, rerr)
		return
	}
	h.withID(w, r, func(ctx context.Context, id uuid.UUID) (any, error) {
		// Checked here for a field-level error; the store guards the channel as well.
		current, err := h.store.GetTemplate(ctx, id)
		if err != nil {
			return nil, err
		}
		if current.Channel != in.Channel {
			return nil, validationFailed([]violation{{Field: "channel",
				Message: fmt.Sprintf("cannot change; the template is %s", current.Channel)}})
		}
		t, err := h.store.UpdateTemplate(ctx, id, in)
		return toTemplateJSON(t), err
	}, http.StatusOK)
}

// deleteTemplate handles DELETE /v1/templates/{id}. Notifications created from it are not
// affected.
func (h *Handler) deleteTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		h.writeError(w, r, domain.ErrNotFound)
		return
	}
	if err := h.store.DeleteTemplate(r.Context(), id); err != nil {
		h.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// listTemplates handles GET /v1/templates: newest first, optionally filtered by channel.
func (h *Handler) listTemplates(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f storage.TemplateFilter
	if v := q.Get("channel"); v != "" {
		c, err := domain.ParseChannel(v)
		if err != nil {
			h.writeError(w, r, badRequest("channel: unknown value %q", v))
			return
		}
		f.Channel = &c
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > storage.MaxListLimit {
			h.writeError(w, r, badRequest("limit: must be an integer from 1 to %d", storage.MaxListLimit))
			return
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		id, ok := decodeCursor(v)
		if !ok {
			h.writeError(w, r, badRequest("cursor: invalid"))
			return
		}
		f.After = &id
	}
	page, err := h.store.ListTemplates(r.Context(), f)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	out := templateListJSON{Items: make([]templateJSON, len(page.Items))}
	for i, t := range page.Items {
		out.Items[i] = toTemplateJSON(t)
	}
	if page.Next != nil {
		out.NextCursor = encodeCursor(*page.Next)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// decodeTemplate reads and validates a template request body.
func (h *Handler) decodeTemplate(w http.ResponseWriter, r *http.Request) (storage.TemplateInput, *requestError) {
	var req templateRequest
	if rerr := decodeJSON(w, r, maxSingleBody, &req); rerr != nil {
		return storage.TemplateInput{}, rerr
	}
	var vs []violation
	if err := domain.ValidateTemplateName(req.Name); err != nil {
		vs = append(vs, violation{Field: "name", Message: message(err)})
	}
	ch, err := domain.ParseChannel(req.Channel)
	switch {
	case req.Channel == "":
		vs = append(vs, violation{Field: "channel", Message: "is required"})
	case err != nil:
		vs = append(vs, violation{Field: "channel", Message: "must be one of sms, email, push"})
	default:
		if _, err := domain.ValidateTemplateBody(ch, req.Body); err != nil {
			vs = append(vs, violation{Field: "body", Message: message(err)})
		}
	}
	if len(vs) > 0 {
		return storage.TemplateInput{}, validationFailed(vs)
	}
	return storage.TemplateInput{Name: req.Name, Channel: ch, Body: req.Body}, nil
}

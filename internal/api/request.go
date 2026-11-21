package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// requestError is a client error detected before the store is called.
type requestError struct {
	status     int
	code       string
	message    string
	violations []violation
}

func (e *requestError) Error() string { return e.message }

// violation is schema FieldViolation.
type violation struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func badRequest(format string, args ...any) *requestError {
	return &requestError{status: http.StatusBadRequest, code: "bad_request", message: fmt.Sprintf(format, args...)}
}

// decodeJSON reads a JSON body of at most limit bytes into dst. Unknown fields and trailing data
// are rejected, so a typo in a field name is an error instead of a silently ignored value.
func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, dst any) *requestError {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		return &requestError{status: http.StatusUnsupportedMediaType, code: "unsupported_media_type",
			message: "the request body must be application/json"}
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var (
			tooLarge *http.MaxBytesError
			typeErr  *json.UnmarshalTypeError
			syntax   *json.SyntaxError
		)
		switch {
		case errors.As(err, &tooLarge):
			return &requestError{status: http.StatusRequestEntityTooLarge, code: "payload_too_large",
				message: fmt.Sprintf("the request body exceeds %d bytes", tooLarge.Limit)}
		case errors.As(err, &typeErr):
			// Report the JSON path and type, not Go struct names.
			return badRequest("field %q must not be a JSON %s", typeErr.Field, typeErr.Value)
		case errors.As(err, &syntax):
			return badRequest("malformed JSON at byte %d", syntax.Offset)
		case strings.HasPrefix(err.Error(), "json: unknown field "):
			return badRequest("unknown field %s", strings.TrimPrefix(err.Error(), "json: unknown field "))
		default:
			return badRequest("malformed JSON body")
		}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return badRequest("malformed JSON body: unexpected data after the JSON value")
	}
	return nil
}

// validateCreate validates one create request and converts it to the store input. prefix is
// prepended to field names (e.g. "notifications[3].") so batch violations point at the item.
// templates holds the templates the request may name, loaded beforehand (see loadTemplates).
func (h *Handler) validateCreate(prefix string, req createRequest, now time.Time, templates map[uuid.UUID]domain.Template) (storage.NewNotification, []violation) {
	var vs []violation
	add := func(field, msg string) { vs = append(vs, violation{Field: prefix + field, Message: msg}) }

	// A template supplies the content and, if the request omits it, the channel (ADR-0010).
	var tpl *domain.Template
	if req.TemplateID != nil {
		if req.Content != "" {
			add("content", "must be omitted when templateId is set")
		}
		switch id, err := uuid.Parse(*req.TemplateID); {
		case err != nil:
			add("templateId", "must be a UUID")
		default:
			if t, ok := templates[id]; ok {
				tpl = &t
			} else {
				add("templateId", "no template with this id")
			}
		}
	} else if req.Variables != nil {
		add("variables", "requires templateId")
	}

	ch, chErr := domain.ParseChannel(req.Channel)
	chOK := false
	switch {
	case req.Channel == "" && tpl != nil:
		ch, chOK = tpl.Channel, true
	case req.Channel == "" && req.TemplateID != nil:
		// The templateId violation already explains it; the channel would come from the template.
	case req.Channel == "":
		add("channel", "is required")
	case chErr != nil:
		add("channel", "must be one of sms, email, push")
	case tpl != nil && ch != tpl.Channel:
		add("channel", fmt.Sprintf("must match the template's channel (%s) or be omitted", tpl.Channel))
	default:
		chOK = true
	}

	content := req.Content
	if chOK {
		if err := domain.ValidateRecipient(ch, req.Recipient); err != nil {
			add("recipient", message(err))
		}
		switch {
		case tpl != nil:
			rendered, err := tpl.Render(req.Variables)
			var re *domain.RenderError
			switch {
			case errors.As(err, &re):
				for _, v := range re.Missing {
					add("variables."+v, "is required by the template")
				}
				for _, v := range re.Unused {
					add("variables."+v, "is not used by the template")
				}
			case err != nil:
				add("templateId", message(err))
			default:
				content = rendered
				if err := domain.ValidateContent(ch, content); err != nil {
					add("content", "rendered from the template: "+message(err))
				}
			}
		case req.TemplateID == nil:
			if err := domain.ValidateContent(ch, content); err != nil {
				add("content", message(err))
			}
		}
	}
	prio, err := domain.ParsePriority(req.Priority)
	if err != nil {
		add("priority", "must be one of high, normal, low")
	}

	var scheduledAt *time.Time
	if req.ScheduledAt != nil {
		// PostgreSQL stores microseconds; normalise so the stored value and the request hash agree.
		// A time in the past is accepted and means "as soon as possible": rejecting it would make
		// an idempotent retry fail with 422 once the original schedule has passed (ADR-0004).
		at := req.ScheduledAt.UTC().Truncate(time.Microsecond)
		if at.After(now.Add(h.opts.MaxScheduleAhead)) {
			add("scheduledAt", fmt.Sprintf("must be at most %s ahead", h.opts.MaxScheduleAhead))
		} else {
			scheduledAt = &at
		}
	}

	in := storage.NewNotification{
		Channel: ch, Priority: prio, Recipient: req.Recipient, Content: content, ScheduledAt: scheduledAt,
	}
	if tpl != nil {
		in.Template = &storage.TemplateRef{ID: tpl.ID, Version: tpl.Version}
	}
	return in, vs
}

// canonicalOf is the form of a create request that idempotency hashes cover (ADR-0004). It is
// built from what the client sent, before validation: a templated request hashes its templateId
// and variables, not the rendered text, so a retry still matches after the template was edited
// or deleted (ADR-0010). For valid plain requests it equals the validated values, so their hashes
// are the same as before templates existed.
func canonicalOf(req createRequest) canonical {
	c := canonical{Channel: req.Channel, Priority: req.Priority, Recipient: req.Recipient, Content: req.Content, Variables: req.Variables}
	if c.Priority == "" {
		c.Priority = string(domain.PriorityNormal)
	}
	if req.ScheduledAt != nil {
		at := req.ScheduledAt.UTC().Truncate(time.Microsecond)
		c.ScheduledAt = &at
	}
	if req.TemplateID != nil {
		c.TemplateID = *req.TemplateID
		if id, err := uuid.Parse(*req.TemplateID); err == nil {
			c.TemplateID = id.String() // one spelling per id
		}
	}
	return c
}

// loadTemplates loads every template the requests name, in one query. Ids that are not UUIDs are
// skipped here and reported by validateCreate.
func (h *Handler) loadTemplates(ctx context.Context, reqs ...createRequest) (map[uuid.UUID]domain.Template, error) {
	var ids []uuid.UUID
	seen := map[uuid.UUID]bool{}
	for _, r := range reqs {
		if r.TemplateID == nil {
			continue
		}
		if id, err := uuid.Parse(*r.TemplateID); err == nil && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return h.store.GetTemplates(ctx, ids)
}

// idempotencyKey reads and checks the Idempotency-Key header. It returns nil if absent.
func idempotencyKey(r *http.Request) (*string, *requestError) {
	k := r.Header.Get(headerIdempotencyKey)
	if k == "" {
		return nil, nil
	}
	if !validKey(k) {
		return nil, badRequest("%s must be 1 to %d printable ASCII characters", headerIdempotencyKey, maxIdempotencyKeyLength)
	}
	return &k, nil
}

func validKey(k string) bool {
	if k == "" || len(k) > maxIdempotencyKeyLength {
		return false
	}
	for i := range len(k) {
		if k[i] < 0x20 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

// canonical is the normalised form of one notification request that idempotency hashes cover.
// Defaults are applied first, so omitting priority and sending "normal" hash the same. The
// template fields are omitted when empty, so plain-content requests hash as they did before
// templates existed.
type canonical struct {
	Channel     string            `json:"channel"`
	Priority    string            `json:"priority"`
	Recipient   string            `json:"recipient"`
	Content     string            `json:"content"`
	ScheduledAt *time.Time        `json:"scheduledAt"`
	Key         string            `json:"key,omitempty"`
	TemplateID  string            `json:"templateId,omitempty"`
	Variables   map[string]string `json:"variables,omitempty"`
}

// hashVersion prefixes every hashed request. Bump it when canonical changes shape, so retries in
// flight during a deploy are not compared against an incompatible encoding by accident.
const hashVersion = "v1\n"

// requestHash is the SHA-256 of the JSON encoding of v (ADR-0004). Struct fields encode in a
// fixed order, so equal requests always produce equal hashes.
func requestHash(v any) [32]byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("api: hash request: %v", err)) // only plain data types are hashed
	}
	return sha256.Sum256(append([]byte(hashVersion), b...))
}

// message returns the human part of a domain validation error.
func message(err error) string {
	return strings.TrimPrefix(err.Error(), domain.ErrInvalidArgument.Error()+": ")
}

package api

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

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
func (h *Handler) validateCreate(prefix string, req createRequest, now time.Time) (storage.NewNotification, []violation) {
	var vs []violation
	add := func(field, msg string) { vs = append(vs, violation{Field: prefix + field, Message: msg}) }

	ch, chErr := domain.ParseChannel(req.Channel)
	switch {
	case req.Channel == "":
		add("channel", "is required")
	case chErr != nil:
		add("channel", "must be one of sms, email, push")
	default:
		if err := domain.ValidateRecipient(ch, req.Recipient); err != nil {
			add("recipient", message(err))
		}
		if err := domain.ValidateContent(ch, req.Content); err != nil {
			add("content", message(err))
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
	return storage.NewNotification{
		Channel: ch, Priority: prio, Recipient: req.Recipient, Content: req.Content, ScheduledAt: scheduledAt,
	}, vs
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
// Defaults are applied first, so omitting priority and sending "normal" hash the same.
type canonical struct {
	Channel     string     `json:"channel"`
	Priority    string     `json:"priority"`
	Recipient   string     `json:"recipient"`
	Content     string     `json:"content"`
	ScheduledAt *time.Time `json:"scheduledAt"`
	Key         string     `json:"key,omitempty"`
}

func canonicalOf(n storage.NewNotification) canonical {
	return canonical{
		Channel: string(n.Channel), Priority: string(n.Priority), Recipient: n.Recipient,
		Content: n.Content, ScheduledAt: n.ScheduledAt,
	}
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

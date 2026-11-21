// Package logging builds the structured slog logger and carries the correlation ID through
// context.Context, so every log line written with a request or job context includes it, and the
// trace and span ids when the context holds a recorded span (B-5), so a log line leads to its
// trace.
package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Log attribute names of the request context (FR-4.2, B-5).
const (
	CorrelationIDKey = "correlation_id"
	TraceIDKey       = "trace_id"
	SpanIDKey        = "span_id"
)

type ctxKey struct{}

// WithCorrelationID returns a copy of ctx carrying the correlation ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the correlation ID stored in ctx, or "" if there is none.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// Options configure New.
type Options struct {
	Level   string // debug, info, warn, error
	Format  string // json or text
	Service string // role of the process: api, dispatcher, worker, ...
	// Instance identifies the process (hostname / container id).
	Instance string
}

// New returns a logger writing to w. Every record carries service and instance attributes,
// and the correlation ID when the record is logged with a context that holds one.
func New(w io.Writer, o Options) *slog.Logger {
	hopts := &slog.HandlerOptions{Level: parseLevel(o.Level)}

	var h slog.Handler
	if strings.EqualFold(o.Format, "text") {
		h = slog.NewTextHandler(w, hopts)
	} else {
		h = slog.NewJSONHandler(w, hopts)
	}

	h = contextHandler{Handler: h}
	l := slog.New(h)
	if o.Service != "" {
		l = l.With("service", o.Service)
	}
	if o.Instance != "" {
		l = l.With("instance", o.Instance)
	}
	return l
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// contextHandler adds the correlation ID and the trace context from the record's context.
type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String(CorrelationIDKey, id))
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && sc.IsSampled() {
		r.AddAttrs(slog.String(TraceIDKey, sc.TraceID().String()), slog.String(SpanIDKey, sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{Handler: h.Handler.WithGroup(name)}
}

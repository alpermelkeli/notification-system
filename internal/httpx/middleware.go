package httpx

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/logging"
)

const (
	// HeaderCorrelationID is accepted from clients and echoed on every response.
	HeaderCorrelationID = "X-Correlation-ID"
	// HeaderInstanceID names the replica that served the request (scaling demo, ADR-0008).
	HeaderInstanceID = "X-Instance-ID"

	maxCorrelationIDLen = 128
)

// CorrelationID reuses a valid X-Correlation-ID from the client or generates a UUIDv7, stores it
// in the request context for logging and echoes it in the response.
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderCorrelationID)
		if !validCorrelationID(id) {
			id = newID()
		}
		w.Header().Set(HeaderCorrelationID, id)
		next.ServeHTTP(w, r.WithContext(logging.WithCorrelationID(r.Context(), id)))
	})
}

// validCorrelationID accepts short IDs made of URL-safe characters only, so client input can
// never inject into logs or headers.
func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationIDLen {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

func newID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

// InstanceID sets the X-Instance-ID response header.
func InstanceID(instance string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(HeaderInstanceID, instance)
			next.ServeHTTP(w, r)
		})
	}
}

// RequestLogger logs one line per request. Probe and scrape endpoints are logged at debug level
// so they do not drown out real traffic.
func RequestLogger(logger *slog.Logger, quietPaths ...string) func(http.Handler) http.Handler {
	quiet := make(map[string]bool, len(quietPaths))
	for _, p := range quietPaths {
		quiet[p] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			level := slog.LevelInfo
			switch {
			case status >= 500:
				level = slog.LevelError
			case quiet[r.URL.Path]:
				level = slog.LevelDebug
			}
			logger.Log(r.Context(), level, "http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"bytes", ww.BytesWritten(),
				"duration_ms", float64(time.Since(start).Microseconds())/1000,
				"remote", r.RemoteAddr,
			)
		})
	}
}

// Recoverer turns a panic into a 500 error envelope and logs the stack trace.
func Recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
					panic(rec)
				}
				logger.ErrorContext(r.Context(), "panic recovered",
					"panic", rec, "stack", string(debug.Stack()))
				WriteError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RoutePattern returns the chi route pattern that served r (e.g. /v1/notifications/{id}), or
// "unmatched". Metrics and span names use it so ids do not explode their cardinality. Call it
// after the router has served r, when the pattern is complete.
func RoutePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if p := rctx.RoutePattern(); p != "" {
			return p
		}
	}
	return "unmatched"
}

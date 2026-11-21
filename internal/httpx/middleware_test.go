package httpx

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpermelkeli/notification-system/internal/logging"
)

func TestCorrelationID(t *testing.T) {
	tests := []struct {
		name     string
		incoming string
		keep     bool
	}{
		{"generated when absent", "", false},
		{"kept when valid", "abc-123_X.y", true},
		{"replaced when it has unsafe characters", "abc\r\ninjected", false},
		{"replaced when too long", strings.Repeat("a", maxCorrelationIDLen+1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen string
			h := CorrelationID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = logging.CorrelationID(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.incoming != "" {
				req.Header.Set(HeaderCorrelationID, tt.incoming)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get(HeaderCorrelationID)
			if got == "" || got != seen {
				t.Fatalf("header %q, context %q", got, seen)
			}
			if tt.keep != (got == tt.incoming) {
				t.Errorf("got %q, incoming %q, keep=%v", got, tt.incoming, tt.keep)
			}
		})
	}
}

func TestInstanceID(t *testing.T) {
	rec := httptest.NewRecorder()
	InstanceID("api-7")(http.NotFoundHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Get(HeaderInstanceID); got != "api-7" {
		t.Errorf("X-Instance-ID = %q", got)
	}
}

func TestRequestLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, logging.Options{Level: "info"})
	h := CorrelationID(RequestLogger(logger, "/healthz")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})))

	for _, path := range []string{"/v1/x", "/healthz", "/boom"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 log lines (healthz is debug), got %d: %s", len(lines), buf.String())
	}
	var first, second map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &first)
	_ = json.Unmarshal([]byte(lines[1]), &second)
	if first["path"] != "/v1/x" || first["status"] != float64(200) || first[logging.CorrelationIDKey] == nil {
		t.Errorf("unexpected first line: %v", first)
	}
	if second["level"] != slog.LevelError.String() || second["status"] != float64(502) {
		t.Errorf("unexpected second line: %v", second)
	}
}

func TestRecoverer(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, logging.Options{})
	h := Recoverer(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("kaboom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	var body ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error.Code != "internal_error" {
		t.Errorf("body = %s (%v)", rec.Body.String(), err)
	}
	if !strings.Contains(buf.String(), "kaboom") {
		t.Errorf("panic not logged: %s", buf.String())
	}
}

func TestWriteErrorEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, http.StatusBadRequest, "validation_failed", "bad", map[string]string{"field": "to"})
	want := `{"error":{"code":"validation_failed","message":"bad","details":[{"field":"to"}]}}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Errorf("got %s\nwant %s", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
}

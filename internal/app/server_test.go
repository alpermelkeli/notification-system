package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/health"
	"github.com/alpermelkeli/notification-system/internal/httpx"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestBaseRouter(t *testing.T) {
	failing := health.Check{Name: "postgres", Probe: func(context.Context) error { return errors.New("down") }}
	r := baseRouter(discard(), "api-1", []health.Check{failing})

	tests := []struct {
		method, path string
		wantCode     int
		wantBody     string
	}{
		{http.MethodGet, "/healthz", http.StatusOK, `"status":"ok"`},
		{http.MethodGet, "/readyz", http.StatusServiceUnavailable, `"postgres":"unavailable"`},
		{http.MethodGet, "/metrics", http.StatusOK, "go_goroutines"},
		{http.MethodGet, "/nope", http.StatusNotFound, `"code":"not_found"`},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, `"code":"method_not_allowed"`},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
			if rec.Code != tt.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tt.wantCode)
			}
			if !strings.Contains(rec.Body.String(), tt.wantBody) {
				t.Errorf("body does not contain %q: %.200s", tt.wantBody, rec.Body.String())
			}
			if rec.Header().Get(httpx.HeaderInstanceID) != "api-1" {
				t.Error("missing X-Instance-ID")
			}
			if rec.Header().Get(httpx.HeaderCorrelationID) == "" {
				t.Error("missing X-Correlation-ID")
			}
		})
	}
}

// TestServeListenerDrainsInFlightRequests checks graceful shutdown: a request that is in
// flight when the context is cancelled still completes.
func TestServeListenerDrainsInFlightRequests(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
	})

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serveListener(ctx, discard(), ln, h, 5*time.Second) }()

	type result struct {
		body string
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String()) //nolint:noctx // test client
		if err != nil {
			got <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		got <- result{string(b), err}
	}()

	<-started
	cancel()

	if r := <-got; r.err != nil || r.body != "done" {
		t.Fatalf("in-flight request: body=%q err=%v", r.body, r.err)
	}
	if err := <-served; err != nil {
		t.Fatalf("serveListener: %v", err)
	}
}

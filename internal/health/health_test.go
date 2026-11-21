package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func ok(context.Context) error { return nil }

func TestLiveness(t *testing.T) {
	rec := httptest.NewRecorder()
	Liveness()(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Errorf("got %d %s", rec.Code, rec.Body.String())
	}
}

func TestReadiness(t *testing.T) {
	slow := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	}
	tests := []struct {
		name       string
		checks     []Check
		wantCode   int
		wantStatus string
		wantChecks map[string]string
	}{
		{
			name:       "no checks",
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantChecks: map[string]string{},
		},
		{
			name:       "all pass",
			checks:     []Check{{"postgres", ok}, {"kafka", ok}},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantChecks: map[string]string{"postgres": "ok", "kafka": "ok"},
		},
		{
			name: "one fails",
			checks: []Check{{"postgres", ok}, {"redis", func(context.Context) error {
				return errors.New("connection refused")
			}}},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "unavailable",
			wantChecks: map[string]string{"postgres": "ok", "redis": "unavailable"},
		},
		{
			name:       "timeout",
			checks:     []Check{{"kafka", slow}},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "unavailable",
			wantChecks: map[string]string{"kafka": "unavailable"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Readiness(slog.New(slog.NewTextHandler(io.Discard, nil)), 50*time.Millisecond, tt.checks...)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if rec.Code != tt.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tt.wantCode)
			}
			var resp Response
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", resp.Status, tt.wantStatus)
			}
			if len(resp.Checks) != len(tt.wantChecks) {
				t.Errorf("checks = %v, want %v", resp.Checks, tt.wantChecks)
			}
			for k, v := range tt.wantChecks {
				if resp.Checks[k] != v {
					t.Errorf("checks[%s] = %q, want %q", k, resp.Checks[k], v)
				}
			}
		})
	}
}

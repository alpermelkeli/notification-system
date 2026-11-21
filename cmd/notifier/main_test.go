package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunCommands(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
		wantErr  string
	}{
		{"no args", nil, 2, "", "usage: notifier"},
		{"unknown", []string{"nope"}, 2, "", `unknown command "nope"`},
		{"help", []string{"help"}, 0, "usage: notifier", ""},
		{"version", []string{"version"}, 0, "dev", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out, errb bytes.Buffer
			if code := run(tt.args, &out, &errb); code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if !strings.Contains(out.String(), tt.wantOut) {
				t.Errorf("stdout %q does not contain %q", out.String(), tt.wantOut)
			}
			if !strings.Contains(errb.String(), tt.wantErr) {
				t.Errorf("stderr %q does not contain %q", errb.String(), tt.wantErr)
			}
		})
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	t.Setenv("LOG_LEVEL", "loud")
	var out, errb bytes.Buffer
	if code := run([]string{"api"}, &out, &errb); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(errb.String(), "LOG_LEVEL") {
		t.Errorf("stderr = %q", errb.String())
	}
}

func TestHealthcheck(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer healthy.Close()
	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unhealthy.Close()

	var errb bytes.Buffer
	if code := healthcheck([]string{healthy.URL}, &errb); code != 0 {
		t.Errorf("healthy: exit %d (%s)", code, errb.String())
	}
	if code := healthcheck([]string{unhealthy.URL}, &errb); code != 1 {
		t.Errorf("unhealthy: exit %d", code)
	}
	if code := healthcheck([]string{"http://127.0.0.1:1"}, &errb); code != 1 {
		t.Errorf("unreachable: exit %d", code)
	}
}

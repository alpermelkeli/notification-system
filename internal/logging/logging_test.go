package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func decode(t *testing.T, b *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b.Bytes(), &m); err != nil {
		t.Fatalf("decode %q: %v", b.String(), err)
	}
	return m
}

func TestNewAddsServiceInstanceAndCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{Level: "info", Service: "api", Instance: "api-1"})

	ctx := WithCorrelationID(context.Background(), "corr-123")
	l.With("k", "v").InfoContext(ctx, "hello")

	m := decode(t, &buf)
	for key, want := range map[string]string{
		"msg": "hello", "service": "api", "instance": "api-1", "k": "v", CorrelationIDKey: "corr-123",
	} {
		if m[key] != want {
			t.Errorf("%s = %v, want %q", key, m[key], want)
		}
	}
}

func TestNoCorrelationIDWithoutContextValue(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, Options{}).InfoContext(context.Background(), "x")
	if _, ok := decode(t, &buf)[CorrelationIDKey]; ok {
		t.Error("correlation_id should be absent")
	}
}

func TestCorrelationIDInsideGroup(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{}).WithGroup("req")
	l.InfoContext(WithCorrelationID(context.Background(), "c1"), "x", "path", "/")
	if !strings.Contains(buf.String(), `"correlation_id":"c1"`) {
		t.Errorf("missing correlation id: %s", buf.String())
	}
}

func TestLevelFiltering(t *testing.T) {
	tests := []struct {
		level     string
		debugSeen bool
		infoSeen  bool
	}{
		{"debug", true, true},
		{"info", false, true},
		{"warn", false, false},
		{"", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			var buf bytes.Buffer
			l := New(&buf, Options{Level: tt.level})
			l.Debug("d")
			gotDebug := strings.Contains(buf.String(), `"msg":"d"`)
			l.Info("i")
			gotInfo := strings.Contains(buf.String(), `"msg":"i"`)
			if gotDebug != tt.debugSeen || gotInfo != tt.infoSeen {
				t.Errorf("debug=%v info=%v, want %v/%v", gotDebug, gotInfo, tt.debugSeen, tt.infoSeen)
			}
		})
	}
}

func TestTextFormat(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, Options{Format: "text"}).Info("hello")
	if !strings.Contains(buf.String(), "msg=hello") {
		t.Errorf("unexpected text output: %s", buf.String())
	}
}

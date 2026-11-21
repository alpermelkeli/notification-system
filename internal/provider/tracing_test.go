package provider

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// secretPath stands for the webhook.site inbox id, which grants read access to the inbox.
const secretPath = "/0e2c5f1a-secret-inbox"

func recordSpans(t *testing.T) (*tracetest.SpanRecorder, trace.Tracer) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
	return rec, tp.Tracer("test")
}

func TestSendTracesWithoutTheURLPath(t *testing.T) {
	rec, tracer := recordSpans(t)
	var gotParent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotParent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c, err := New(srv.URL+secretPath, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	ctx, parent := tracer.Start(context.Background(), "process")
	if _, err := c.Send(ctx, Message{To: "+905551112233", Channel: "sms", Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	parent.End()

	var client sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindClient {
			client = s
		}
	}
	if client == nil || client.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatalf("no client span under the caller's span: %v", client)
	}
	if !strings.Contains(gotParent, client.SpanContext().SpanID().String()) {
		t.Errorf("provider got traceparent %q, want the client span %s", gotParent, client.SpanContext().SpanID())
	}
	if strings.Contains(client.Name(), secretPath) {
		t.Errorf("span name %q leaks the URL path", client.Name())
	}
	for _, a := range client.Attributes() {
		if strings.Contains(a.Value.String(), secretPath) {
			t.Errorf("attribute %s leaks the URL path: %s", a.Key, a.Value.String())
		}
	}
}

func TestTransportErrorsDoNotCarryTheURLPath(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: connection refused

	c, err := New("http://"+addr+secretPath, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Send(context.Background(), Message{To: "+905551112233", Channel: "sms", Content: "hi"})
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if strings.Contains(err.Error(), secretPath) || !strings.Contains(err.Error(), addr) {
		t.Errorf("error %q must name the host but not the path", err)
	}
}

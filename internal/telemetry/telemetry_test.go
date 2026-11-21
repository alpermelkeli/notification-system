package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func sampledSpan(t *testing.T) (context.Context, trace.Span) {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp.Tracer("test").Start(context.Background(), "request")
}

func TestTraceParentRoundTrip(t *testing.T) {
	ctx, span := sampledSpan(t)
	defer span.End()

	tp := TraceParent(ctx)
	if len(tp) != 55 || tp[:3] != "00-" || tp[len(tp)-3:] != "-01" {
		t.Fatalf("traceparent = %q", tp)
	}
	got := trace.SpanContextFromContext(WithTraceParent(context.Background(), tp))
	if !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("restored span context = %+v, want the request span", got)
	}
}

func TestTraceParentWithoutARecordedSpan(t *testing.T) {
	if tp := TraceParent(context.Background()); tp != "" {
		t.Errorf("traceparent without a span = %q", tp)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	ctx, span := tp.Tracer("test").Start(context.Background(), "unsampled")
	defer span.End()
	got := TraceParent(ctx)
	if len(got) != 55 || got[len(got)-3:] != "-00" {
		t.Fatalf("traceparent of an unsampled span = %q, want it stored with the sampled flag off", got)
	}
	if sc := trace.SpanContextFromContext(WithTraceParent(context.Background(), got)); !sc.IsValid() || sc.IsSampled() {
		t.Errorf("restored %+v, want a valid unsampled parent", sc)
	}
	for _, bad := range []string{"", "garbage", "00-zz-yy-01"} {
		if sc := trace.SpanContextFromContext(WithTraceParent(context.Background(), bad)); sc.IsValid() {
			t.Errorf("WithTraceParent(%q) produced %+v", bad, sc)
		}
	}
}

func TestRecordCarrierRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx, span := sampledSpan(t)
	defer span.End()

	r := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "correlation_id", Value: []byte("c-1")}}}
	InjectRecord(ctx, r)
	InjectRecord(ctx, r) // re-injecting replaces the header instead of adding a second one
	if len(r.Headers) != 2 {
		t.Fatalf("headers = %+v", r.Headers)
	}
	got := trace.SpanContextFromContext(ExtractRecord(context.Background(), r))
	if got.TraceID() != span.SpanContext().TraceID() || !got.IsRemote() {
		t.Errorf("extracted %+v, want trace %s", got, span.SpanContext().TraceID())
	}
	if sc := trace.SpanContextFromContext(ExtractRecord(context.Background(), &kgo.Record{})); sc.IsValid() {
		t.Errorf("a record without headers produced %+v", sc)
	}
}

func TestSetupWithoutEndpointIsNoop(t *testing.T) {
	shutdown, err := Setup(context.Background(), Options{Service: "api"})
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, span := Tracer().Start(context.Background(), "x")
	if span.SpanContext().IsSampled() {
		t.Error("spans must not be recorded without an endpoint")
	}
}

func TestSetupExportsOverOTLPHTTP(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/traces" {
			posts.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })

	shutdown, err := Setup(context.Background(), Options{Endpoint: srv.URL + "/", SampleRatio: 1, Service: "worker", Instance: "w-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, span := Tracer().Start(context.Background(), "deliver")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if posts.Load() == 0 {
		t.Error("no spans were posted to /v1/traces")
	}
}

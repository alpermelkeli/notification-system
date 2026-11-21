package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/alpermelkeli/notification-system/internal/httpx"
)

func TestHTTPMiddlewareNamesSpansAfterTheRoute(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})

	var inner trace.SpanContext
	r := chi.NewRouter()
	r.Use(httpx.CorrelationID, HTTPMiddleware("/healthz"))
	r.Get("/healthz", func(http.ResponseWriter, *http.Request) {})
	r.Route("/v1", func(r chi.Router) {
		r.Get("/notifications/{id}", func(w http.ResponseWriter, req *http.Request) {
			inner = trace.SpanContextFromContext(req.Context())
			w.WriteHeader(http.StatusInternalServerError)
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/notifications/abc", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	req.Header.Set(httpx.HeaderCorrelationID, "corr-1")
	r.ServeHTTP(httptest.NewRecorder(), req)
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("%d spans, want 1 (probes are not traced)", len(spans))
	}
	s := spans[0]
	if s.Name() != "GET /v1/notifications/{id}" || s.SpanKind() != trace.SpanKindServer {
		t.Errorf("span %q kind %v", s.Name(), s.SpanKind())
	}
	if s.Parent().TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || s.Parent().SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("parent = %v, want the client's traceparent", s.Parent())
	}
	if inner.SpanID() != s.SpanContext().SpanID() {
		t.Error("the handler does not see the server span in its context")
	}
	attrs := map[string]string{}
	for _, a := range s.Attributes() {
		attrs[string(a.Key)] = a.Value.String()
	}
	if attrs["http.route"] != "/v1/notifications/{id}" || attrs["http.response.status_code"] != "500" || attrs["correlation_id"] != "corr-1" {
		t.Errorf("attributes = %v", attrs)
	}
	if s.Status().Code != codes.Error {
		t.Errorf("status = %v, want error for a 500", s.Status())
	}
}

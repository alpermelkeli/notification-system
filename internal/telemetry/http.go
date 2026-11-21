package telemetry

import (
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/alpermelkeli/notification-system/internal/httpx"
	"github.com/alpermelkeli/notification-system/internal/logging"
)

// HTTPMiddleware starts a server span per request, continuing a traceparent sent by the client.
// The span is named after the chi route pattern once routing is done ("POST /v1/notifications")
// and carries the status and the correlation ID, so a trace can be found from an API response.
// Requests to skipPaths (probes, scrapes) are not traced. It must run inside a chi router, after
// httpx.CorrelationID.
//
// otelhttp's handler is not used here: it names the span after http.Request.Pattern, which chi
// sets to the mount point ("/v1/*") for routes in a sub-router.
func HTTPMiddleware(skipPaths ...string) func(http.Handler) http.Handler {
	skip := make(map[string]bool, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skip[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			ctx, span := Tracer().Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(r.Method),
					semconv.URLPath(r.URL.Path),
				))
			defer span.End()

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r.WithContext(ctx))

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			route := httpx.RoutePattern(r) // the route context is shared, so it is complete here
			span.SetName(r.Method + " " + route)
			span.SetAttributes(semconv.HTTPRoute(route), semconv.HTTPResponseStatusCode(status))
			if id := logging.CorrelationID(r.Context()); id != "" {
				span.SetAttributes(AttrCorrelationID.String(id))
			}
			if status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(status))
			}
		})
	}
}

// HTTPTransport wraps base so each outgoing request gets a client span and a traceparent header.
func HTTPTransport(base http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(base)
}

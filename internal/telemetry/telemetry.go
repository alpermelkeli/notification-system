// Package telemetry sets up OpenTelemetry tracing (B-5, ADR-0009) and carries trace context
// across the two hops that are not HTTP: the notification row, which links the API request to
// every later dispatch, and the Kafka record, which links a dispatch to its delivery.
//
// Setup installs the process-wide tracer provider and the W3C propagator once, in main; packages
// then take tracers with otel.Tracer, as OpenTelemetry instrumentation libraries do. Without an
// OTLP endpoint the provider stays the no-op one: spans cost nothing and no trace context is
// stored or propagated.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope of the notifier's own spans.
const TracerName = "github.com/alpermelkeli/notification-system"

// Tracer returns the notifier's tracer from the installed provider.
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

// Options configures Setup.
type Options struct {
	// Endpoint is the base URL of an OTLP/HTTP collector, e.g. http://jaeger:4318. Spans are
	// posted to Endpoint/v1/traces. Empty disables tracing.
	Endpoint string
	// SampleRatio is the share of new traces recorded, 0 to 1. Spans with a parent follow the
	// parent's decision, so a trace is recorded whole or not at all.
	SampleRatio float64
	// Service names the role (api, dispatcher, worker); the trace service is notifier-<role>.
	Service  string
	Instance string
	Version  string
}

// Setup installs the W3C trace-context propagator and, if o.Endpoint is set, a batching tracer
// provider exporting over OTLP/HTTP. The returned function flushes and stops the provider; call
// it on shutdown with a bounded context.
func Setup(ctx context.Context, o Options) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if o.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(strings.TrimRight(o.Endpoint, "/")+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName("notifier-"+o.Service),
		semconv.ServiceInstanceID(o.Instance),
		semconv.ServiceVersion(o.Version),
	))
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, fmt.Errorf("build trace resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(o.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

const traceparentKey = "traceparent"

// TraceParent returns the W3C traceparent of the span in ctx, or "" if there is none. It is
// stored with a notification so later dispatches continue the trace. An unsampled span is stored
// too: its sampled flag tells the dispatcher and the worker not to record the notification's
// spans either, so a trace is never recorded in part.
func TraceParent(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	c := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, c)
	return c[traceparentKey]
}

// WithTraceParent returns ctx with the remote span context of a stored traceparent as parent.
// An empty or malformed value leaves ctx unchanged.
func WithTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{traceparentKey: traceParent})
}

// Attributes shared by the notifier's spans.
const (
	AttrNotificationID = attribute.Key("notification.id")
	AttrChannel        = attribute.Key("notification.channel")
	AttrPriority       = attribute.Key("notification.priority")
	AttrAttempt        = attribute.Key("notification.attempt")
	AttrCorrelationID  = attribute.Key("correlation_id")
)

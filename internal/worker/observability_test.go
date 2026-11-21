package worker

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/retry"
	"github.com/alpermelkeli/notification-system/internal/telemetry"
)

func TestOutcomesAreCounted(t *testing.T) {
	reg := metrics.NewRegistry()
	h := newHarness(t, nil, Options{
		Metrics: metrics.NewDelivery(reg),
		Retry:   retry.Policy{MaxAttempts: 3, Rand: halfJitter},
	})
	h.sender.fn = func(m provider.Message) (provider.Result, error) {
		switch m.To {
		case "flaky":
			return provider.Result{}, &provider.Error{StatusCode: http.StatusServiceUnavailable}
		case "invalid":
			return provider.Result{}, &provider.Error{StatusCode: http.StatusBadRequest}
		case "throttled":
			return provider.Result{}, &provider.Error{StatusCode: http.StatusTooManyRequests, RetryAfter: time.Second}
		}
		return provider.Result{MessageID: "m"}, nil
	}
	var ns []domain.Notification
	for _, to := range []string{"ok", "flaky", "invalid", "throttled"} {
		n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
		h.store.mu.Lock()
		h.store.rows[n.ID].Recipient = to
		h.store.mu.Unlock()
		ns = append(ns, n)
	}
	ns = append(ns, h.store.add(domain.PriorityNormal, domain.StatusCancelled, 0))
	for _, n := range ns {
		h.feed(t, n, 0)
	}
	eventually(t, "all handled", func() bool { return h.committedAt(normalTopic, 0) == 5 })

	want := `
# HELP notifier_dead_letters_total Dead letters of failed notifications, by publishing component and result (published, failed).
# TYPE notifier_dead_letters_total counter
notifier_dead_letters_total{result="published",source="worker"} 1
# HELP notifier_delivery_outcomes_total Work records handled by the workers, by channel and outcome.
# TYPE notifier_delivery_outcomes_total counter
notifier_delivery_outcomes_total{channel="sms",outcome="failed"} 1
notifier_delivery_outcomes_total{channel="sms",outcome="released"} 1
notifier_delivery_outcomes_total{channel="sms",outcome="retrying"} 1
notifier_delivery_outcomes_total{channel="sms",outcome="sent"} 1
notifier_delivery_outcomes_total{channel="sms",outcome="skipped"} 1
# HELP notifier_provider_requests_total Provider calls by channel and outcome class (delivered, retryable, rate_limited, permanent).
# TYPE notifier_provider_requests_total counter
notifier_provider_requests_total{channel="sms",class="delivered"} 1
notifier_provider_requests_total{channel="sms",class="permanent"} 1
notifier_provider_requests_total{channel="sms",class="rate_limited"} 1
notifier_provider_requests_total{channel="sms",class="retryable"} 1
# HELP notifier_worker_sends_in_flight Sends in progress (claim, provider call, outcome write) by channel.
# TYPE notifier_worker_sends_in_flight gauge
notifier_worker_sends_in_flight{channel="sms"} 0
`
	names := []string{"notifier_dead_letters_total", "notifier_delivery_outcomes_total", "notifier_provider_requests_total", "notifier_worker_sends_in_flight"}
	// The outcome is counted after the watermark can move; wait for the last counter.
	eventually(t, "counters", func() bool {
		return testutil.GatherAndCompare(reg, strings.NewReader(want), names...) == nil
	})
	for _, name := range []string{"notifier_delivery_latency_seconds", "notifier_queue_wait_seconds", "notifier_provider_request_duration_seconds"} {
		if testutil.CollectAndCount(reg, name) == 0 {
			t.Errorf("%s has no series", name)
		}
	}
	if got := sample(t, reg, "notifier_worker_buffered_records", "priority", "normal"); got != 0 {
		t.Errorf("buffered after draining = %v", got)
	}
}

func TestBreakerStateIsExported(t *testing.T) {
	reg := metrics.NewRegistry()
	h := newHarness(t, nil, Options{
		Metrics: metrics.NewDelivery(reg),
		Breaker: BreakerOptions{ConsecutiveFailures: 1, OpenTimeout: time.Hour},
		Retry:   retry.Policy{MaxAttempts: 5, Rand: halfJitter},
	})
	if got := sample(t, reg, "notifier_circuit_breaker_state", "channel", "sms"); got != metrics.BreakerClosed {
		t.Fatalf("initial state = %v", got)
	}
	h.sender.fn = func(provider.Message) (provider.Result, error) {
		return provider.Result{}, &provider.Error{StatusCode: http.StatusBadGateway}
	}
	h.feed(t, h.store.add(domain.PriorityNormal, domain.StatusQueued, 0), 0)
	eventually(t, "breaker open", func() bool {
		return sample(t, reg, "notifier_circuit_breaker_state", "channel", "sms") == metrics.BreakerOpen
	})
	if got := sample(t, reg, "notifier_circuit_breaker_transitions_total", "to", "open"); got != 1 {
		t.Errorf("transitions to open = %v", got)
	}
}

func TestDeliverySpansContinueTheDispatch(t *testing.T) {
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

	h := newHarness(t, nil, Options{})
	n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	r, err := queue.NewMessage(n).Record(n.CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	pctx, publish := tp.Tracer("test").Start(context.Background(), "send", trace.WithSpanKind(trace.SpanKindProducer))
	telemetry.InjectRecord(pctx, r)
	publish.End()
	h.w.enqueue(r)
	eventually(t, "delivered", func() bool { return h.committedAt(normalTopic, 0) == 1 })
	h.stop()

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		byName[s.Name()] = s
	}
	process := byName["process "+normalTopic]
	if process == nil {
		t.Fatalf("no process span among %v", byName)
	}
	if process.Parent().SpanID() != publish.SpanContext().SpanID() || process.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("process span parent %v kind %v, want the producer span", process.Parent().SpanID(), process.SpanKind())
	}
	for _, child := range []string{"claim", "write outcome"} {
		s := byName[child]
		if s == nil || s.Parent().SpanID() != process.SpanContext().SpanID() {
			t.Errorf("%s span is not a child of the process span: %v", child, s)
		}
	}
	attrs := map[string]string{}
	for _, a := range process.Attributes() {
		attrs[string(a.Key)] = a.Value.String()
	}
	if attrs["notification.outcome"] != "sent" || attrs["notification.id"] != n.ID.String() || attrs["correlation_id"] != n.CorrelationID {
		t.Errorf("process span attributes = %v", attrs)
	}
}

// sample returns the value of the gauge or counter series of name that has the label, or -1.
func sample(t *testing.T, reg *prometheus.Registry, name, label, value string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					if m.GetGauge() != nil {
						return m.GetGauge().GetValue()
					}
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return -1
}

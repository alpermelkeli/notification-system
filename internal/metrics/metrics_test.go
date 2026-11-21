package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestHTTPMiddlewareLabelsByRoutePattern(t *testing.T) {
	reg := NewRegistry()
	m := NewHTTP(reg)
	r := chi.NewRouter()
	r.Use(m.Middleware())
	r.Get("/v1/notifications/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })
	r.Post("/v1/notifications", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })

	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/notifications/a", nil),
		httptest.NewRequest(http.MethodGet, "/v1/notifications/b", nil),
		httptest.NewRequest(http.MethodPost, "/v1/notifications", nil),
		httptest.NewRequest(http.MethodGet, "/nope", nil),
		httptest.NewRequest("AAAA1", "/nope", nil),
		httptest.NewRequest("AAAA2", "/nope", nil),
	} {
		r.ServeHTTP(httptest.NewRecorder(), req)
	}

	for _, tc := range []struct {
		labels []string
		want   float64
	}{
		{[]string{"GET", "/v1/notifications/{id}", "404"}, 2},
		{[]string{"POST", "/v1/notifications", "202"}, 1},
		{[]string{"GET", "unmatched", "404"}, 1},
		// Made-up methods share one series instead of one each.
		{[]string{"_OTHER", "unmatched", "405"}, 2},
	} {
		if got := testutil.ToFloat64(m.requests.WithLabelValues(tc.labels...)); got != tc.want {
			t.Errorf("requests%v = %v, want %v", tc.labels, got, tc.want)
		}
	}
	if got := testutil.CollectAndCount(m.duration); got != 4 {
		t.Errorf("duration series = %d, want 4 (one per method and route)", got)
	}
	if got := testutil.ToFloat64(m.inFlight); got != 0 {
		t.Errorf("in flight after the requests = %v", got)
	}
}

func TestNilMetricSetsAreNoops(t *testing.T) {
	var (
		h *HTTP
		d *Delivery
		p *Dispatch
	)
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	if h.Middleware()(next) == nil {
		t.Fatal("nil middleware")
	}
	h.Created(domain.ChannelSMS, domain.PriorityHigh, 1)
	d.ProviderCall(domain.ChannelSMS, "delivered", time.Second)
	d.Outcome(domain.ChannelSMS, OutcomeSent)
	d.Delivered(domain.ChannelSMS, domain.PriorityHigh, time.Second)
	d.Picked(domain.ChannelSMS, domain.PriorityHigh, time.Second)
	d.SendStarted(domain.ChannelSMS)
	d.SendFinished(domain.ChannelSMS)
	d.Buffered(domain.ChannelSMS, domain.PriorityHigh, 1)
	d.BreakerState(domain.ChannelSMS, BreakerOpen, "open")
	d.DBRetry(domain.ChannelSMS, "claim")
	d.DeadLetters(1, nil)
	p.Dispatched([]domain.Notification{{}})
	p.DispatchFailed()
	p.Reaped(ReapStuck, 1)
	p.DeadLetters(1, nil)
	p.Depth(nil, 0, 0)
	p.Lag("g", "t", 1)
	p.DepthFailed("kafka")
}

func TestDeadLettersAndDepth(t *testing.T) {
	reg := NewRegistry()
	d := NewDispatch(reg)
	d.DeadLetters(3, nil)
	d.DeadLetters(2, errors.New("kafka down"))
	d.Depth(map[domain.Status]int{domain.StatusPending: 7, domain.StatusQueued: 2}, 5, -1)
	d.Lag("workers.sms", "notifications.sms.high", 42)

	want := `
# HELP notifier_dead_letters_total Dead letters of failed notifications, by publishing component and result (published, failed).
# TYPE notifier_dead_letters_total counter
notifier_dead_letters_total{result="failed",source="reaper"} 2
notifier_dead_letters_total{result="published",source="reaper"} 3
# HELP notifier_notifications Unfinished notifications in PostgreSQL by status (pending, queued, processing, retrying).
# TYPE notifier_notifications gauge
notifier_notifications{status="pending"} 7
notifier_notifications{status="queued"} 2
# HELP notifier_oldest_due_age_seconds How long the longest-waiting due notification has waited for dispatch; 0 when none is due.
# TYPE notifier_oldest_due_age_seconds gauge
notifier_oldest_due_age_seconds 0
# HELP notifier_kafka_consumer_lag Work records not yet committed by the channel's consumer group, by topic: the workers' backlog.
# TYPE notifier_kafka_consumer_lag gauge
notifier_kafka_consumer_lag{group="workers.sms",topic="notifications.sms.high"} 42
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"notifier_dead_letters_total", "notifier_notifications", "notifier_oldest_due_age_seconds", "notifier_kafka_consumer_lag"); err != nil {
		t.Error(err)
	}
}

func TestHandlerServesTheRegistry(t *testing.T) {
	reg := NewRegistry()
	NewDelivery(reg).Outcome(domain.ChannelPush, OutcomeSent)
	rec := httptest.NewRecorder()
	Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{`notifier_delivery_outcomes_total{channel="push",outcome="sent"} 1`, "go_goroutines", "process_"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

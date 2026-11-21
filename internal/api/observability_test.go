package api

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/alpermelkeli/notification-system/internal/httpx"
	"github.com/alpermelkeli/notification-system/internal/metrics"
)

// TestCreateStoresTheOriginAndCountsCreatedNotifications checks that a create passes the
// request's traceparent to the store, so the dispatcher can continue the trace, and that only new
// notifications are counted.
func TestCreateStoresTheOriginAndCountsCreatedNotifications(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(t.Context()) }()

	store := newFakeStore()
	reg := metrics.NewRegistry()
	m := metrics.NewHTTP(reg)
	r := chi.NewRouter()
	r.Use(httpx.CorrelationID, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx, span := tp.Tracer("test").Start(req.Context(), "request")
			defer span.End()
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	New(store, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		MaxScheduleAhead: 24 * time.Hour, Metrics: m, Now: func() time.Time { return testNow },
	}).Routes(r)
	s := &testServer{store: store, h: r}

	s.do(t, http.MethodPost, "/v1/notifications", otp, "Idempotency-Key", "k1").wantStatus(http.StatusAccepted)
	if o := store.lastOrigin; len(o.TraceParent) != 55 || !strings.HasPrefix(o.TraceParent, "00-") || o.CorrelationID == "" {
		t.Errorf("origin passed to the store = %+v", o)
	}
	s.do(t, http.MethodPost, "/v1/notifications", otp, "Idempotency-Key", "k1").wantStatus(http.StatusAccepted)

	batch := `{"notifications":[` + otp + `,{"recipient":"user@example.com","channel":"email","content":"hi"}]}`
	s.do(t, http.MethodPost, "/v1/notifications/batch", batch).wantStatus(http.StatusAccepted)

	want := `
# HELP notifier_notifications_created_total Notifications accepted by the API, by channel and priority. Idempotent replays are not counted.
# TYPE notifier_notifications_created_total counter
notifier_notifications_created_total{channel="email",priority="normal"} 1
notifier_notifications_created_total{channel="sms",priority="high"} 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "notifier_notifications_created_total"); err != nil {
		t.Error(err)
	}
}

package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/httpx"
)

// HTTP holds the metrics of an HTTP server and of the API.
type HTTP struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge
	created  *prometheus.CounterVec
}

// NewHTTP registers the HTTP metrics on reg.
func NewHTTP(reg prometheus.Registerer) *HTTP {
	m := &HTTP{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total",
			Help: "HTTP requests by method, route pattern and status code.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help:    "HTTP request latency by method and route pattern.",
			Buckets: requestBuckets,
		}, []string{"method", "route"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "http_requests_in_flight",
			Help: "HTTP requests being served.",
		}),
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "notifications_created_total",
			Help: "Notifications accepted by the API, by channel and priority. Idempotent replays are not counted.",
		}, []string{"channel", "priority"}),
	}
	reg.MustRegister(m.requests, m.duration, m.inFlight, m.created)
	return m
}

// Middleware records every request under its chi route pattern (e.g. /v1/notifications/{id}),
// so ids do not explode the label set. It must run inside a chi router.
func (m *HTTP) Middleware(next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		m.inFlight.Inc()
		defer m.inFlight.Dec()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		route := httpx.RoutePattern(r)
		m.requests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
		m.duration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

// Created counts notifications the API stored.
func (m *HTTP) Created(c domain.Channel, p domain.Priority, n int) {
	if m == nil || n == 0 {
		return
	}
	m.created.WithLabelValues(string(c), string(p)).Add(float64(n))
}

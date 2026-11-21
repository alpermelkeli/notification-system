package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// Reasons the reaper recovers a notification.
const (
	ReapExpiredLease   = "expired_lease"
	ReapExhaustedLease = "exhausted_lease"
	ReapConsumed       = "consumed"
	ReapStuck          = "stuck"
)

// Dispatch holds the metrics of a dispatcher process: relay, reaper and queue depth.
type Dispatch struct {
	dispatched     *prometheus.CounterVec
	dispatchErrors prometheus.Counter
	reaped         *prometheus.CounterVec
	deadLetters    *prometheus.CounterVec

	notifications *prometheus.GaugeVec
	due           prometheus.Gauge
	oldestDueAge  prometheus.Gauge
	lag           *prometheus.GaugeVec
	depthErrors   *prometheus.CounterVec
}

// NewDispatch registers the dispatcher metrics on reg.
func NewDispatch(reg prometheus.Registerer) *Dispatch {
	m := &Dispatch{
		dispatched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "dispatched_total",
			Help: "Notifications published to the Kafka work topics, by channel and priority. Counted on publish, before the relay commits; a failed commit leaves duplicates that the workers discard.",
		}, []string{"channel", "priority"}),
		dispatchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "dispatch_errors_total",
			Help: "Relay batches that failed (Kafka or PostgreSQL unavailable); their rows stay due.",
		}),
		reaped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "reaper_recovered_total",
			Help: "Notifications recovered by the reaper, by reason (expired_lease, exhausted_lease, consumed, stuck).",
		}, []string{"reason"}),
		deadLetters: deadLetterVec(reg),
		notifications: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "notifications",
			Help: "Unfinished notifications in PostgreSQL by status (pending, queued, processing, retrying).",
		}, []string{"status"}),
		due: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "notifications_due",
			Help: "Pending and retrying notifications that are due now: the dispatcher's backlog.",
		}),
		oldestDueAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "oldest_due_age_seconds",
			Help: "How long the longest-waiting due notification has waited for dispatch; 0 when none is due.",
		}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "kafka_consumer_lag",
			Help: "Work records not yet committed by the channel's consumer group, by topic: the workers' backlog.",
		}, []string{"group", "topic"}),
		depthErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "queue_depth_errors_total",
			Help: "Failed queue depth samples by source (postgres, kafka); the gauges keep their last value.",
		}, []string{"source"}),
	}
	reg.MustRegister(m.dispatched, m.dispatchErrors, m.reaped, m.notifications, m.due, m.oldestDueAge,
		m.lag, m.depthErrors)
	return m
}

// Dispatched counts published notifications.
func (m *Dispatch) Dispatched(ns []domain.Notification) {
	if m == nil {
		return
	}
	for _, n := range ns {
		m.dispatched.WithLabelValues(string(n.Channel), string(n.Priority)).Inc()
	}
}

// DispatchFailed counts a failed relay batch.
func (m *Dispatch) DispatchFailed() {
	if m != nil {
		m.dispatchErrors.Inc()
	}
}

// Reaped counts n notifications recovered for reason.
func (m *Dispatch) Reaped(reason string, n int) {
	if m != nil && n > 0 {
		m.reaped.WithLabelValues(reason).Add(float64(n))
	}
}

// DeadLetters counts n dead letters published by the reaper, or not published if err is set.
func (m *Dispatch) DeadLetters(n int, err error) {
	if m != nil {
		countDeadLetters(m.deadLetters, "reaper", n, err)
	}
}

// Depth sets the PostgreSQL backlog gauges.
func (m *Dispatch) Depth(byStatus map[domain.Status]int, due int, oldestDueAge float64) {
	if m == nil {
		return
	}
	for st, n := range byStatus {
		m.notifications.WithLabelValues(string(st)).Set(float64(n))
	}
	m.due.Set(float64(due))
	m.oldestDueAge.Set(max(oldestDueAge, 0))
}

// Lag sets the consumer lag of one group's topic.
func (m *Dispatch) Lag(group, topic string, lag int64) {
	if m != nil {
		m.lag.WithLabelValues(group, topic).Set(float64(lag))
	}
}

// DepthFailed counts a failed depth sample.
func (m *Dispatch) DepthFailed(source string) {
	if m != nil {
		m.depthErrors.WithLabelValues(source).Inc()
	}
}

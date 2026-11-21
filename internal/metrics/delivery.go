package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// Outcome is what a worker did with one work record.
type Outcome string

// Outcomes of a work record.
const (
	OutcomeSent     Outcome = "sent"
	OutcomeRetrying Outcome = "retrying"
	OutcomeFailed   Outcome = "failed"
	// OutcomeReleased hands the attempt back without consuming it: a 429, the breaker opening
	// after the claim, or no rate-limit token.
	OutcomeReleased Outcome = "released"
	// OutcomeSkipped is a record whose notification was cancelled, finished or claimed already.
	OutcomeSkipped Outcome = "skipped"
	// OutcomeUnpersisted is a send whose outcome could not be written; the reaper retries it
	// after the lease.
	OutcomeUnpersisted Outcome = "unpersisted"
	// OutcomeAbandoned is a claim given up on shutdown or revoke during a database outage.
	OutcomeAbandoned Outcome = "abandoned"
	// OutcomeClaimFailed is a claim that failed with a non-transient error; the reaper
	// re-publishes the notification.
	OutcomeClaimFailed Outcome = "claim_failed"
)

// Breaker states as exported by the circuit breaker gauge.
const (
	BreakerClosed   = 0
	BreakerHalfOpen = 1
	BreakerOpen     = 2
)

// Delivery holds the metrics of a worker process.
type Delivery struct {
	providerRequests   *prometheus.CounterVec
	providerDuration   *prometheus.HistogramVec
	outcomes           *prometheus.CounterVec
	latency            *prometheus.HistogramVec
	queueWait          *prometheus.HistogramVec
	inFlight           *prometheus.GaugeVec
	buffered           *prometheus.GaugeVec
	breakerState       *prometheus.GaugeVec
	breakerTransitions *prometheus.CounterVec
	dbRetries          *prometheus.CounterVec
	deadLetters        *prometheus.CounterVec
}

// NewDelivery registers the worker metrics on reg.
func NewDelivery(reg prometheus.Registerer) *Delivery {
	m := &Delivery{
		providerRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "provider_requests_total",
			Help: "Provider calls by channel and outcome class (delivered, retryable, rate_limited, permanent).",
		}, []string{"channel", "class"}),
		providerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "provider_request_duration_seconds",
			Help:    "Provider call latency by channel and outcome class.",
			Buckets: requestBuckets,
		}, []string{"channel", "class"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "delivery_outcomes_total",
			Help: "Work records handled by the workers, by channel and outcome.",
		}, []string{"channel", "outcome"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "delivery_latency_seconds",
			Help:    "Time from when a notification was first due (created or scheduled) to its delivery, across retries.",
			Buckets: latencyBuckets,
		}, []string{"channel", "priority"}),
		queueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "queue_wait_seconds",
			Help:    "Time from when a dispatch became due to when a worker picked it, by channel and priority.",
			Buckets: latencyBuckets,
		}, []string{"channel", "priority"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "worker_sends_in_flight",
			Help: "Sends in progress (claim, provider call, outcome write) by channel.",
		}, []string{"channel"}),
		buffered: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "worker_buffered_records",
			Help: "Work records fetched from Kafka and waiting in the worker's priority heaps.",
		}, []string{"channel", "priority"}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "circuit_breaker_state",
			Help: "Circuit breaker state per channel: 0 closed, 1 half-open, 2 open.",
		}, []string{"channel"}),
		breakerTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "circuit_breaker_transitions_total",
			Help: "Circuit breaker state changes by channel and new state.",
		}, []string{"channel", "to"}),
		dbRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "worker_db_retries_total",
			Help: "Claims and outcome writes retried after a transient database error, by channel and operation.",
		}, []string{"channel", "operation"}),
		deadLetters: deadLetterVec(reg),
	}
	reg.MustRegister(m.providerRequests, m.providerDuration, m.outcomes, m.latency, m.queueWait,
		m.inFlight, m.buffered, m.breakerState, m.breakerTransitions, m.dbRetries)
	return m
}

// ProviderCall records one provider call.
func (m *Delivery) ProviderCall(c domain.Channel, class string, d time.Duration) {
	if m == nil {
		return
	}
	m.providerRequests.WithLabelValues(string(c), class).Inc()
	m.providerDuration.WithLabelValues(string(c), class).Observe(d.Seconds())
}

// Outcome counts what happened to one work record.
func (m *Delivery) Outcome(c domain.Channel, o Outcome) {
	if m == nil {
		return
	}
	m.outcomes.WithLabelValues(string(c), string(o)).Inc()
}

// Delivered records the end-to-end latency of a delivered notification.
func (m *Delivery) Delivered(c domain.Channel, p domain.Priority, latency time.Duration) {
	if m == nil {
		return
	}
	m.latency.WithLabelValues(string(c), string(p)).Observe(max(latency, 0).Seconds())
}

// Picked records how long a dispatch waited from due to pick.
func (m *Delivery) Picked(c domain.Channel, p domain.Priority, wait time.Duration) {
	if m == nil {
		return
	}
	m.queueWait.WithLabelValues(string(c), string(p)).Observe(max(wait, 0).Seconds())
}

// SendStarted and SendFinished track the sends in flight.
func (m *Delivery) SendStarted(c domain.Channel) {
	if m != nil {
		m.inFlight.WithLabelValues(string(c)).Inc()
	}
}

// SendFinished is the counterpart of SendStarted.
func (m *Delivery) SendFinished(c domain.Channel) {
	if m != nil {
		m.inFlight.WithLabelValues(string(c)).Dec()
	}
}

// Buffered sets the number of buffered records of a priority.
func (m *Delivery) Buffered(c domain.Channel, p domain.Priority, n int) {
	if m != nil {
		m.buffered.WithLabelValues(string(c), string(p)).Set(float64(n))
	}
}

// BreakerState sets the breaker gauge; to names the new state for the transition counter, or
// is empty for the initial state.
func (m *Delivery) BreakerState(c domain.Channel, state int, to string) {
	if m == nil {
		return
	}
	m.breakerState.WithLabelValues(string(c)).Set(float64(state))
	if to != "" {
		m.breakerTransitions.WithLabelValues(string(c), to).Inc()
	}
}

// DBRetry counts a database operation (claim, outcome) retried after a transient error.
func (m *Delivery) DBRetry(c domain.Channel, operation string) {
	if m != nil {
		m.dbRetries.WithLabelValues(string(c), operation).Inc()
	}
}

// DeadLetters counts n dead letters published by a worker, or not published if err is set.
func (m *Delivery) DeadLetters(n int, err error) {
	if m != nil {
		countDeadLetters(m.deadLetters, "worker", n, err)
	}
}

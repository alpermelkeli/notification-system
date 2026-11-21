package metrics

import "github.com/prometheus/client_golang/prometheus"

// Results of publishing a status event.
const (
	EventPublished = "published"
	EventFailed    = "failed"  // not acknowledged within the delivery timeout
	EventDropped   = "dropped" // the producer's buffer was full
)

// Events holds the metrics of a status event publisher (ADR-0011). Every role that changes
// statuses has one.
type Events struct {
	results *prometheus.CounterVec
}

// NewEvents registers the status event metrics on reg.
func NewEvents(reg prometheus.Registerer) *Events {
	m := &Events{results: prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "status_events_total",
		Help: "Status events published to notification.events, by result (published, failed, dropped). Events are best effort: failed and dropped ones are lost.",
	}, []string{"result"})}
	for _, r := range []string{EventPublished, EventFailed, EventDropped} {
		m.results.WithLabelValues(r) // export zeros, so rates and alerts work from the start
	}
	reg.MustRegister(m.results)
	return m
}

// Result counts one event by its publish result.
func (m *Events) Result(result string) {
	if m != nil {
		m.results.WithLabelValues(result).Inc()
	}
}

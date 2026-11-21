// Package metrics defines the Prometheus metrics of the notifier roles (FR-4.1, ADR-0009).
//
// Each role builds its own registry with NewRegistry and serves it on /metrics; components get
// their metric set through their options. Every method is safe on a nil receiver, so components
// run without metrics in tests. All names start with notifier_.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "notifier"

// NewRegistry returns a registry with the Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Handler serves reg in the Prometheus exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// Histogram buckets, in seconds.
var (
	// requestBuckets fit API requests and provider calls: 5 ms to 10 s.
	requestBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	// latencyBuckets fit end-to-end delivery, which includes the queue backlog and retry
	// backoff: 50 ms to 1 h.
	latencyBuckets = []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 900, 1800, 3600}
)

// deadLetterVec counts dead letters by the component that published them and the result.
func deadLetterVec(reg prometheus.Registerer) *prometheus.CounterVec {
	v := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "dead_letters_total",
		Help: "Dead letters of failed notifications, by publishing component and result (published, failed).",
	}, []string{"source", "result"})
	reg.MustRegister(v)
	return v
}

func countDeadLetters(v *prometheus.CounterVec, source string, n int, err error) {
	result := "published"
	if err != nil {
		result = "failed"
	}
	v.WithLabelValues(source, result).Add(float64(n))
}

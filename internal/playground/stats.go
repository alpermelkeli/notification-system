package playground

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpermelkeli/notification-system/internal/httpx"
)

// Stats is the summary the page polls. Rates are per second over the last 20 seconds; with the
// 5s scrape interval of the compose Prometheus they trail reality by a few seconds.
type Stats struct {
	// Available is false when Prometheus is not configured or did not answer.
	Available bool   `json:"available"`
	Error     string `json:"error,omitempty"`
	// Instances counts the healthy replicas of each role (api, dispatcher, worker).
	Instances map[string]float64 `json:"instances"`
	// Notifications counts unfinished rows in PostgreSQL by status, as last sampled by the
	// dispatcher.
	Notifications map[string]float64 `json:"notifications"`
	// KafkaLag is the workers' backlog per work topic.
	KafkaLag map[string]float64 `json:"kafkaLag"`
	// CreatedPerSecond is the rate of notifications accepted by the API.
	CreatedPerSecond float64 `json:"createdPerSecond"`
	// DeliveredPerSecond is the rate of provider calls that delivered, per channel.
	DeliveredPerSecond map[string]float64 `json:"deliveredPerSecond"`
	// ProviderCalls counts provider calls since the workers started, by channel and outcome
	// class (delivered, retryable, rate_limited, permanent).
	ProviderCalls map[string]map[string]float64 `json:"providerCalls"`
	// Breakers is the worst circuit breaker state per channel across workers: closed,
	// half_open or open.
	Breakers map[string]string `json:"breakers"`
	// DeadLetters counts dead letters published since start.
	DeadLetters float64   `json:"deadLetters"`
	At          time.Time `json:"at"`
}

// statsQueries are the PromQL expressions behind Stats. Gauges exported by every dispatcher
// replica take max, not sum, because each replica samples the same database and topics.
var statsQueries = map[string]string{
	"instances":     `sum by (job) (up)`,
	"notifications": `max by (status) (notifier_notifications)`,
	"lag":           `max by (topic) (notifier_kafka_consumer_lag)`,
	"created":       `sum(rate(notifier_notifications_created_total[20s]))`,
	"delivered":     `sum by (channel) (rate(notifier_provider_requests_total{class="delivered"}[20s]))`,
	"calls":         `sum by (channel, class) (notifier_provider_requests_total)`,
	"breakers":      `max by (channel) (notifier_circuit_breaker_state)`,
	"deadLetters":   `sum(notifier_dead_letters_total{result="published"})`,
}

// statsCacheTTL lets many open pages share one set of Prometheus queries.
const statsCacheTTL = time.Second

type statsSource struct {
	client  *http.Client
	baseURL string
	now     func() time.Time

	mu      sync.Mutex
	cached  Stats
	expires time.Time
}

func newStatsSource(client *http.Client, baseURL string, now func() time.Time) *statsSource {
	return &statsSource{client: client, baseURL: strings.TrimRight(baseURL, "/"), now: now}
}

func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	st := h.stats.get(r.Context())
	if st.Error != "" {
		h.logger.Debug("playground stats unavailable", "err", st.Error)
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, st)
}

// get returns the cached summary, refreshing it when it is older than statsCacheTTL. The lock is
// held during the refresh, so concurrent requests wait for one set of queries.
func (s *statsSource) get(ctx context.Context) Stats {
	if s.baseURL == "" {
		return Stats{Error: "DEMO_PROMETHEUS_URL is not set", At: s.now().UTC()}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.now().Before(s.expires) {
		return s.cached
	}
	// A client that goes away must not leave a failed summary cached for everyone else; the
	// HTTP client's timeout still bounds the queries.
	s.cached = s.fetch(context.WithoutCancel(ctx))
	s.expires = s.now().Add(statsCacheTTL)
	return s.cached
}

func (s *statsSource) fetch(ctx context.Context) Stats {
	type result struct {
		name    string
		samples []sample
		err     error
	}
	results := make(chan result, len(statsQueries))
	for name, q := range statsQueries {
		go func() {
			samples, err := s.query(ctx, q)
			results <- result{name, samples, err}
		}()
	}
	byName := make(map[string][]sample, len(statsQueries))
	var errs []error
	for range statsQueries {
		r := <-results
		if r.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.name, r.err))
			continue
		}
		byName[r.name] = r.samples
	}
	st := Stats{
		Available:          len(errs) == 0,
		Instances:          byLabel(byName["instances"], "job"),
		Notifications:      byLabel(byName["notifications"], "status"),
		KafkaLag:           byLabel(byName["lag"], "topic"),
		CreatedPerSecond:   scalar(byName["created"]),
		DeliveredPerSecond: byLabel(byName["delivered"], "channel"),
		ProviderCalls:      map[string]map[string]float64{},
		Breakers:           map[string]string{},
		DeadLetters:        scalar(byName["deadLetters"]),
		At:                 s.now().UTC(),
	}
	if err := errors.Join(errs...); err != nil {
		st.Error = err.Error()
	}
	for _, sm := range byName["calls"] {
		ch := sm.labels["channel"]
		if st.ProviderCalls[ch] == nil {
			st.ProviderCalls[ch] = map[string]float64{}
		}
		st.ProviderCalls[ch][sm.labels["class"]] = sm.value
	}
	for _, sm := range byName["breakers"] {
		st.Breakers[sm.labels["channel"]] = breakerName(sm.value)
	}
	return st
}

func breakerName(v float64) string {
	switch v {
	case 1:
		return "half_open"
	case 2:
		return "open"
	default:
		return "closed"
	}
}

type sample struct {
	labels map[string]string
	value  float64
}

// query runs an instant PromQL query and returns its vector samples.
func (s *statsSource) query(ctx context.Context, q string) ([]sample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/api/v1/query?query="+url.QueryEscape(q), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus answered %d", resp.StatusCode)
	}
	var body struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if body.Status != "success" || body.Data.ResultType != "vector" {
		return nil, fmt.Errorf("unexpected prometheus result %q/%q", body.Status, body.Data.ResultType)
	}
	out := make([]sample, 0, len(body.Data.Result))
	for _, r := range body.Data.Result {
		str, ok := r.Value[1].(string)
		if !ok {
			return nil, errors.New("prometheus sample value is not a string")
		}
		v, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return nil, fmt.Errorf("prometheus sample value %q: %w", str, err)
		}
		out = append(out, sample{labels: r.Metric, value: v})
	}
	return out, nil
}

func byLabel(samples []sample, label string) map[string]float64 {
	out := make(map[string]float64, len(samples))
	for _, s := range samples {
		out[s.labels[label]] = s.value
	}
	return out
}

func scalar(samples []sample) float64 {
	if len(samples) == 0 {
		return 0
	}
	return samples[0].value
}

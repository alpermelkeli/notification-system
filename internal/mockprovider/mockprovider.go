// Package mockprovider is a local stand-in for the external delivery provider (webhook.site).
//
// It speaks the provider contract from the assessment brief
// (POST {to, channel, content} -> 202 {messageId, status, timestamp}) and can inject latency,
// 5xx failures and 429 rate limiting, so retries, the circuit breaker and the rate limiter can be
// exercised locally and in load tests without calling the real webhook.site (ADR-0006).
package mockprovider

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// Config controls fault injection.
type Config struct {
	// Latency is the base processing delay; a uniform random delay in [0, Jitter) is added.
	Latency time.Duration
	Jitter  time.Duration
	// FailureRate is the probability (0..1) of answering 500.
	FailureRate float64
	// RateLimitRate is the probability (0..1) of answering 429 with Retry-After.
	RateLimitRate float64
	// RetryAfter is the Retry-After value sent with 429 responses.
	RetryAfter time.Duration
}

// Request is the provider request body.
type Request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Response is the provider's 202 body.
type Response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// Stats is returned by GET /stats.
type Stats struct {
	// Accepted counts 202 responses per channel since start.
	Accepted map[string]int64 `json:"accepted"`
	// Rejected counts simulated 429/500 responses per channel since start.
	Rejected map[string]int64 `json:"rejected"`
	// RateLastSecond is the number of requests received per channel in the last full second,
	// counted on arrival (before the simulated latency) and whatever the outcome, which is what a
	// provider's own rate limit counts. Counting at response time would shift each request by its
	// random latency and show bunching that the sender never produced.
	// In the scaling demo it stays at or below the 100 msg/s limit whatever the worker count.
	RateLastSecond map[string]int64 `json:"rate_last_second"`
}

const maxRemembered = 100_000

var channels = map[string]bool{"sms": true, "email": true, "push": true}

// Server is the mock provider.
type Server struct {
	cfg  Config
	rand func() float64
	now  func() time.Time

	mu        sync.Mutex
	messageID map[string]string // Idempotency-Key -> messageId
	accepted  map[string]int64
	rejected  map[string]int64
	window    map[string]*secondCounter

	requests *prometheus.CounterVec
}

// New creates a server and registers its metrics with reg.
func New(cfg Config, reg prometheus.Registerer) *Server {
	s := &Server{
		cfg:       cfg,
		rand:      rand.Float64,
		now:       time.Now,
		messageID: make(map[string]string),
		accepted:  make(map[string]int64),
		rejected:  make(map[string]int64),
		window:    make(map[string]*secondCounter),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mockprovider_requests_total",
			Help: "Requests received by the mock provider, by channel and result.",
		}, []string{"channel", "result"}),
	}
	reg.MustRegister(s.requests)
	return s
}

// Handler returns the HTTP handler: POST to any path delivers, GET /stats and GET /healthz
// report state.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("POST /", s.handleSend)
	return mux
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var req Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.To == "" || req.Content == "" || !channels[req.Channel] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "to, content and a valid channel are required"})
		return
	}

	s.arrive(req.Channel)
	if !s.sleep(r) {
		return // client gave up
	}

	switch roll := s.rand(); {
	case roll < s.cfg.RateLimitRate:
		s.record(req.Channel, "rate_limited", false)
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(s.cfg.RetryAfter.Seconds()))))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
		return
	case roll < s.cfg.RateLimitRate+s.cfg.FailureRate:
		s.record(req.Channel, "failed", false)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "simulated provider failure"})
		return
	}

	id := s.acceptedID(r.Header.Get("Idempotency-Key"))
	s.record(req.Channel, "accepted", true)
	writeJSON(w, http.StatusAccepted, Response{
		MessageID: id,
		Status:    "accepted",
		Timestamp: s.now().UTC().Format(time.RFC3339Nano),
	})
}

// sleep simulates provider latency. It returns false if the request was cancelled meanwhile.
func (s *Server) sleep(r *http.Request) bool {
	d := s.cfg.Latency
	if s.cfg.Jitter > 0 {
		d += time.Duration(s.rand() * float64(s.cfg.Jitter))
	}
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-r.Context().Done():
		return false
	}
}

// acceptedID returns the messageId for a request. Requests repeating an Idempotency-Key get the
// original messageId, like a real provider deduplicating retries (ADR-0004).
func (s *Server) acceptedID(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != "" {
		if id, ok := s.messageID[key]; ok {
			return id
		}
	}
	id := uuid.NewString()
	if key != "" {
		if len(s.messageID) >= maxRemembered {
			s.messageID = make(map[string]string) // bounded memory; good enough for a mock
		}
		s.messageID[key] = id
	}
	return id
}

// arrive counts a request in its channel's per-second window.
func (s *Server) arrive(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.window[channel]
	if !ok {
		c = &secondCounter{}
		s.window[channel] = c
	}
	c.add(s.now().Unix())
}

func (s *Server) record(channel, result string, accepted bool) {
	s.requests.WithLabelValues(channel, result).Inc()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !accepted {
		s.rejected[channel]++
		return
	}
	s.accepted[channel]++
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	now := s.now().Unix()
	s.mu.Lock()
	st := Stats{
		Accepted:       make(map[string]int64, len(s.accepted)),
		Rejected:       make(map[string]int64, len(s.rejected)),
		RateLastSecond: make(map[string]int64, len(s.window)),
	}
	for k, v := range s.accepted {
		st.Accepted[k] = v
	}
	for k, v := range s.rejected {
		st.Rejected[k] = v
	}
	for k, c := range s.window {
		st.RateLastSecond[k] = c.count(now - 1)
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, st)
}

// secondCounter counts events in the current and the previous second.
type secondCounter struct {
	sec       int64
	cur, prev int64
}

func (c *secondCounter) add(sec int64) {
	switch sec {
	case c.sec:
	case c.sec + 1:
		c.prev, c.cur = c.cur, 0
	default:
		c.prev, c.cur = 0, 0
	}
	c.sec = sec
	c.cur++
}

// count returns the events counted in second sec, if it is still tracked.
func (c *secondCounter) count(sec int64) int64 {
	switch sec {
	case c.sec:
		return c.cur
	case c.sec - 1:
		return c.prev
	default:
		return 0
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

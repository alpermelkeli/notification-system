package worker

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/provider"
)

var errUnavailable = &provider.Error{StatusCode: http.StatusServiceUnavailable}

func TestBreakerOpensAndHoldsTheBacklog(t *testing.T) {
	h := newHarness(t, nil, Options{Concurrency: 1, Breaker: BreakerOptions{ConsecutiveFailures: 3, OpenTimeout: time.Hour}})
	h.sender.fn = func(provider.Message) (provider.Result, error) { return provider.Result{}, errUnavailable }
	var ns []domain.Notification
	for range 6 {
		n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
		ns = append(ns, n)
		h.feed(t, n, 0)
	}
	eventually(t, "three failed sends", func() bool { return h.committedAt(normalTopic, 0) == 3 })
	time.Sleep(200 * time.Millisecond)

	if sent := len(h.sender.sent()); sent != 3 {
		t.Errorf("sent %d, want 3: an open breaker must stop sending", sent)
	}
	if st := h.w.circuit.state(); st != gobreaker.StateOpen {
		t.Errorf("breaker state = %s, want open", st)
	}
	// The other three are neither attempted nor released: they stay buffered behind the
	// watermark, so they wait in Kafka and keep their attempts.
	for _, n := range ns[3:] {
		if got := h.store.outcome(n.ID); got != "" {
			t.Errorf("%s: outcome %q while the breaker is open", n.ID, got)
		}
	}
	if got := h.committedAt(normalTopic, 0); got != 3 {
		t.Errorf("committed %d, want the watermark held at 3", got)
	}
}

func TestBreakerProbesAndCloses(t *testing.T) {
	h := newHarness(t, nil, Options{Concurrency: 4, Breaker: BreakerOptions{ConsecutiveFailures: 2, OpenTimeout: 100 * time.Millisecond}})
	var calls, concurrent, maxConcurrent atomic.Int64
	healthy := make(chan struct{})
	h.sender.fn = func(provider.Message) (provider.Result, error) {
		c := concurrent.Add(1)
		defer concurrent.Add(-1)
		for {
			m := maxConcurrent.Load()
			if c <= m || maxConcurrent.CompareAndSwap(m, c) {
				break
			}
		}
		calls.Add(1)
		select {
		case <-healthy:
			return provider.Result{MessageID: "m"}, nil
		default:
			return provider.Result{}, errUnavailable
		}
	}
	for range 2 {
		h.feed(t, h.store.add(domain.PriorityNormal, domain.StatusQueued, 0), 0)
	}
	eventually(t, "breaker open", func() bool { return h.w.circuit.state() == gobreaker.StateOpen })
	close(healthy)
	var ns []domain.Notification
	for range 10 {
		n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
		ns = append(ns, n)
		h.feed(t, n, 0)
	}
	eventually(t, "all committed", func() bool { return h.committedAt(normalTopic, 0) == 12 })
	for _, n := range ns {
		if got := h.store.outcome(n.ID); got != "sent m" {
			t.Errorf("%s: outcome %q after the breaker closed", n.ID, got)
		}
	}
	if st := h.w.circuit.state(); st != gobreaker.StateClosed {
		t.Errorf("breaker state = %s, want closed", st)
	}
	if c := calls.Load(); c != 12 {
		t.Errorf("provider calls = %d, want 12: nothing released or sent twice", c)
	}
}

func TestBreakerIgnoresPermanentAndRateLimitedAnswers(t *testing.T) {
	h := newHarness(t, nil, Options{Breaker: BreakerOptions{ConsecutiveFailures: 2}})
	var calls atomic.Int64
	h.sender.fn = func(provider.Message) (provider.Result, error) {
		if calls.Add(1)%2 == 0 {
			return provider.Result{}, &provider.Error{StatusCode: http.StatusTooManyRequests}
		}
		return provider.Result{}, &provider.Error{StatusCode: http.StatusBadRequest}
	}
	for range 8 {
		h.feed(t, h.store.add(domain.PriorityNormal, domain.StatusQueued, 0), 0)
	}
	eventually(t, "all committed", func() bool { return h.committedAt(normalTopic, 0) == 8 })
	if st := h.w.circuit.state(); st != gobreaker.StateClosed {
		t.Errorf("breaker state = %s: 4xx and 429 answers must not open it", st)
	}
}

// TestClaimedRecordIsReleasedWhenTheBreakerOpens: a send admitted before the breaker opened is
// not attempted once it is open; the attempt is handed back.
func TestClaimedRecordIsReleasedWhenTheBreakerOpens(t *testing.T) {
	h := newHarness(t, nil, Options{Breaker: BreakerOptions{ConsecutiveFailures: 1, OpenTimeout: time.Hour}})
	n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	h.stop() // drive process directly
	rec := h.feed(t, n, 0)

	admitted, err := h.w.circuit.allow()
	if err != nil {
		t.Fatal(err)
	}
	trip, err := h.w.circuit.allow()
	if err != nil {
		t.Fatal(err)
	}
	trip.done(errUnavailable) // another send fails: the breaker opens
	if !h.w.circuit.open() {
		t.Fatal("breaker did not open")
	}

	h.w.mu.Lock()
	it, _ := h.w.sched.pick(time.Now())
	h.w.mu.Unlock()
	if it == nil || it.rec.kr != rec {
		t.Fatal("record not buffered")
	}
	h.w.process(context.Background(), it.rec, admitted)
	if got := h.store.outcome(n.ID); got != "released 1h0m0s" {
		t.Errorf("outcome %q, want released until the breaker's open period ends", got)
	}
	if len(h.sender.sent()) != 0 {
		t.Error("sent through an open breaker")
	}
}

func TestBreakerOptionsDefaults(t *testing.T) {
	var o BreakerOptions
	o.defaults()
	if o != (BreakerOptions{ConsecutiveFailures: 5, FailureRatio: 0.5, MinRequests: 20, Window: time.Minute, OpenTimeout: 30 * time.Second}) {
		t.Errorf("defaults = %+v", o)
	}
}

func TestBreakerTripsOnFailureRatio(t *testing.T) {
	c := newCircuit("t", BreakerOptions{ConsecutiveFailures: 100, FailureRatio: 0.5, MinRequests: 10, Window: time.Minute, OpenTimeout: time.Hour}, discard)
	for i := range 10 {
		a, err := c.allow()
		if err != nil {
			t.Fatalf("send %d rejected early: %v", i, err)
		}
		if i%3 == 2 { // successes at 2, 5, 8: 7 of 10 fail, the 10th being a failure
			a.done(nil)
		} else {
			a.done(errors.New("connection refused"))
		}
	}
	if !c.open() {
		t.Errorf("breaker state = %s, want open at 70%% failures over 10 sends", c.state())
	}
}

// TestSendAdmittedBeforeATripIsRevokedInHalfOpen: only the probe may reach a recovering
// provider; a send admitted while closed and delayed past the open period is released.
func TestSendAdmittedBeforeATripIsRevokedInHalfOpen(t *testing.T) {
	c := newCircuit("t", BreakerOptions{ConsecutiveFailures: 1, OpenTimeout: 50 * time.Millisecond}, discard)
	early, err := c.allow()
	if err != nil || early.probe {
		t.Fatalf("closed admission: %+v, %v", early, err)
	}
	trip, _ := c.allow()
	trip.done(errUnavailable)
	time.Sleep(80 * time.Millisecond)
	probe, err := c.allow()
	if err != nil || !probe.probe {
		t.Fatalf("half-open admission: probe=%v, %v", probe.probe, err)
	}
	if !c.revoked(early) {
		t.Error("a send admitted before the trip must not go ahead in half-open")
	}
	if c.revoked(probe) {
		t.Error("the probe must go ahead")
	}
	early.done(errNotAttempted)
	probe.done(nil)
	if c.state() != gobreaker.StateClosed {
		t.Errorf("state after a successful probe = %s", c.state())
	}
}

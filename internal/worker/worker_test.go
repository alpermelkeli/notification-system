package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/queue"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeStore keeps notifications in memory with the same guards as the real store.
type fakeStore struct {
	mu       sync.Mutex
	rows     map[uuid.UUID]*domain.Notification
	outcomes map[uuid.UUID]string // "sent", "retrying 5s", "failed"
	claimErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: map[uuid.UUID]*domain.Notification{}, outcomes: map[uuid.UUID]string{}}
}

func (f *fakeStore) add(pr domain.Priority, status domain.Status, attempts int) domain.Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := domain.NewIDAt(time.Now())
	n := &domain.Notification{
		ID: id, CreatedAt: domain.IDTime(id), Channel: domain.ChannelSMS, Priority: pr,
		Recipient: "+905551112233", Content: "hi", Status: status, Attempts: attempts,
		NextAttemptAt: time.Now(), CorrelationID: "corr-" + id.String()[:8],
	}
	f.rows[id] = n
	return *n
}

func (f *fakeStore) outcome(id uuid.UUID) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.outcomes[id]
}

func (f *fakeStore) Claim(_ context.Context, id uuid.UUID, _ time.Time, _ time.Duration) (domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return domain.Notification{}, f.claimErr
	}
	n, ok := f.rows[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	if n.Status != domain.StatusQueued {
		return domain.Notification{}, &domain.TransitionError{Current: n.Status, Target: domain.StatusProcessing}
	}
	tok := uuid.New()
	n.Status, n.Attempts, n.LeaseToken = domain.StatusProcessing, n.Attempts+1, &tok
	return *n, nil
}

func (f *fakeStore) finish(l domain.Lease, to domain.Status, outcome string) (domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.rows[l.NotificationID]
	if n.LeaseToken == nil || *n.LeaseToken != l.Token {
		return domain.Notification{}, domain.ErrLeaseLost
	}
	n.Status, n.LeaseToken = to, nil
	f.outcomes[n.ID] = outcome
	return *n, nil
}

func (f *fakeStore) MarkSent(_ context.Context, l domain.Lease, id string) (domain.Notification, error) {
	return f.finish(l, domain.StatusSent, "sent "+id)
}

func (f *fakeStore) MarkRetrying(_ context.Context, l domain.Lease, d time.Duration, _ string) (domain.Notification, error) {
	return f.finish(l, domain.StatusRetrying, "retrying "+d.String())
}

func (f *fakeStore) MarkFailed(_ context.Context, l domain.Lease, _ string) (domain.Notification, error) {
	return f.finish(l, domain.StatusFailed, "failed")
}

// fakeSender records the order of sends and answers with fn.
type fakeSender struct {
	mu    sync.Mutex
	order []string
	fn    func(provider.Message) (provider.Result, error)
}

func (s *fakeSender) Send(_ context.Context, m provider.Message) (provider.Result, error) {
	s.mu.Lock()
	s.order = append(s.order, m.CorrelationID)
	fn := s.fn
	s.mu.Unlock()
	if fn != nil {
		return fn(m)
	}
	return provider.Result{MessageID: "m"}, nil
}

func (s *fakeSender) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.order)
}

// fakeLimiter grants a token for every value sent on tokens, or always when tokens is nil.
type fakeLimiter struct {
	tokens    chan struct{}
	err       error
	mu        sync.Mutex
	cooldowns []time.Duration
}

func (l *fakeLimiter) Wait(ctx context.Context, _ domain.Channel) error {
	if l.err != nil {
		return l.err
	}
	if l.tokens == nil {
		return nil
	}
	select {
	case <-l.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *fakeLimiter) Cooldown(_ context.Context, _ domain.Channel, d time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cooldowns = append(l.cooldowns, d)
	return nil
}

type fakeFetch struct {
	mu      sync.Mutex
	paused  map[string]bool
	history []string
}

func (f *fakeFetch) PauseFetchTopics(ts ...string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range ts {
		f.paused[t] = true
		f.history = append(f.history, "pause "+t)
	}
	return nil
}

func (f *fakeFetch) ResumeFetchTopics(ts ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range ts {
		delete(f.paused, t)
		f.history = append(f.history, "resume "+t)
	}
}

func (f *fakeFetch) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.history)
}

// harness runs a worker engine without Kafka; records are fed with enqueue.
type harness struct {
	w       *Worker
	store   *fakeStore
	sender  *fakeSender
	limiter *fakeLimiter
	fetch   *fakeFetch

	mu        sync.Mutex
	committed map[string]map[int32]int64
	offsets   map[int32]int64 // next offset per partition
	cancel    context.CancelFunc
	done      chan struct{}
}

func newHarness(t *testing.T, limiter *fakeLimiter, opts Options) *harness {
	t.Helper()
	if limiter == nil {
		limiter = &fakeLimiter{}
	}
	h := &harness{
		store: newFakeStore(), sender: &fakeSender{}, limiter: limiter,
		fetch:     &fakeFetch{paused: map[string]bool{}},
		committed: map[string]map[int32]int64{}, offsets: map[int32]int64{},
		done: make(chan struct{}),
	}
	opts.CommitInterval = 10 * time.Millisecond
	h.w = New(domain.ChannelSMS, h.store, h.sender, limiter, discard, opts)
	h.w.fetch = h.fetch
	h.w.commit = func(_ context.Context, offsets map[string]map[int32]kgo.EpochOffset) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		for topic, ps := range offsets {
			if h.committed[topic] == nil {
				h.committed[topic] = map[int32]int64{}
			}
			for p, eo := range ps {
				h.committed[topic][p] = eo.Offset
			}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		h.w.run(ctx, func(ctx context.Context) bool { <-ctx.Done(); return false })
		close(h.done)
	}()
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.cancel()
	<-h.done
}

// feed publishes n's work record on partition p of its topic and returns the record.
func (h *harness) feed(t *testing.T, n domain.Notification, p int32) *kgo.Record {
	t.Helper()
	r, err := queue.NewMessage(n).Record(n.CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	r.Partition, r.Offset = p, h.offsets[p]
	h.offsets[p]++
	h.mu.Unlock()
	h.w.enqueue(r)
	return r
}

func (h *harness) committedAt(topic string, p int32) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if o, ok := h.committed[topic][p]; ok {
		return o
	}
	return -1
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var normalTopic = queue.Topic(domain.ChannelSMS, domain.PriorityNormal)

func TestDeliversAndCommitsWatermark(t *testing.T) {
	h := newHarness(t, nil, Options{})
	var ns []domain.Notification
	for range 5 {
		n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
		ns = append(ns, n)
		h.feed(t, n, 0)
	}
	eventually(t, "all delivered and committed", func() bool { return h.committedAt(normalTopic, 0) == 5 })
	for _, n := range ns {
		if got := h.store.outcome(n.ID); got != "sent m" {
			t.Errorf("%s: outcome %q", n.ID, got)
		}
	}
	// The provider got the idempotency key and correlation id of each notification.
	if sent := h.sender.sent(); len(sent) != 5 || sent[0] != ns[0].CorrelationID {
		t.Errorf("sent = %v", sent)
	}
}

func TestFailureOutcomes(t *testing.T) {
	h := newHarness(t, nil, Options{RetryDelay: 5 * time.Second, MaxAttempts: 3})
	h.sender.fn = func(m provider.Message) (provider.Result, error) {
		switch m.To {
		case "throttled":
			return provider.Result{}, &provider.Error{StatusCode: http.StatusTooManyRequests, RetryAfter: 9 * time.Second}
		default:
			return provider.Result{}, &provider.Error{StatusCode: http.StatusServiceUnavailable}
		}
	}
	first := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	last := h.store.add(domain.PriorityNormal, domain.StatusQueued, 2) // this claim is attempt 3 of 3
	throttled := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	h.store.mu.Lock()
	h.store.rows[throttled.ID].Recipient = "throttled"
	h.store.mu.Unlock()
	for _, n := range []domain.Notification{first, last, throttled} {
		h.feed(t, n, 0)
	}
	eventually(t, "outcomes", func() bool { return h.committedAt(normalTopic, 0) == 3 })

	if got := h.store.outcome(first.ID); got != "retrying 5s" {
		t.Errorf("first attempt: %q", got)
	}
	if got := h.store.outcome(last.ID); got != "failed" {
		t.Errorf("last attempt: %q", got)
	}
	if got := h.store.outcome(throttled.ID); got != "retrying 9s" {
		t.Errorf("429: %q, want the Retry-After as delay", got)
	}
	h.limiter.mu.Lock()
	defer h.limiter.mu.Unlock()
	if !slices.Equal(h.limiter.cooldowns, []time.Duration{9 * time.Second}) {
		t.Errorf("cool-downs = %v, want one fleet-wide 9s cool-down", h.limiter.cooldowns)
	}
}

func TestStaleAndUndecodableRecordsAreSkipped(t *testing.T) {
	h := newHarness(t, nil, Options{})
	cancelled := h.store.add(domain.PriorityNormal, domain.StatusCancelled, 0)
	h.feed(t, cancelled, 0)
	// A record whose row is pending again (requeued by the reaper) is a stale duplicate.
	h.feed(t, h.store.add(domain.PriorityNormal, domain.StatusPending, 0), 0)
	h.feed(t, domain.Notification{ID: domain.NewIDAt(time.Now()), CreatedAt: time.Now(), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}, 0) // unknown id
	h.w.enqueue(&kgo.Record{Topic: normalTopic, Partition: 0, Offset: 3, Value: []byte("garbage")})
	h.offsets[0] = 4

	eventually(t, "skips committed", func() bool { return h.committedAt(normalTopic, 0) == 4 })
	if sent := h.sender.sent(); len(sent) != 0 {
		t.Errorf("stale records were sent: %v", sent)
	}
}

func TestClaimErrorLeavesRowForTheReaper(t *testing.T) {
	h := newHarness(t, nil, Options{})
	h.store.claimErr = errors.New("connection refused")
	h.feed(t, h.store.add(domain.PriorityNormal, domain.StatusQueued, 0), 0)
	// The record is committed past; the row stays queued and the offset reaper re-publishes it.
	eventually(t, "commit", func() bool { return h.committedAt(normalTopic, 0) == 1 })
	if len(h.sender.sent()) != 0 {
		t.Error("sent without a claim")
	}
}

// TestPriorityDecidedWhenTokenArrives: records buffered while waiting for a token compete, so a
// high record that arrives last is still sent first.
func TestPriorityDecidedWhenTokenArrives(t *testing.T) {
	lim := &fakeLimiter{tokens: make(chan struct{})}
	h := newHarness(t, lim, Options{Concurrency: 1})
	low := h.store.add(domain.PriorityLow, domain.StatusQueued, 0)
	normal := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	high := h.store.add(domain.PriorityHigh, domain.StatusQueued, 0)
	h.feed(t, low, 0)
	h.feed(t, normal, 1)
	h.feed(t, high, 2)
	for range 3 {
		lim.tokens <- struct{}{}
	}
	eventually(t, "three sends", func() bool { return len(h.sender.sent()) == 3 })
	want := []string{high.CorrelationID, normal.CorrelationID, low.CorrelationID}
	if got := h.sender.sent(); !slices.Equal(got, want) {
		t.Errorf("send order = %v, want high, normal, low %v", got, want)
	}
}

func TestBackpressurePausesAndResumesTopic(t *testing.T) {
	lim := &fakeLimiter{tokens: make(chan struct{})}
	h := newHarness(t, lim, Options{HighWater: 3, LowWater: 2, Concurrency: 1})
	for range 3 {
		h.feed(t, h.store.add(domain.PriorityNormal, domain.StatusQueued, 0), 0)
	}
	if ev := h.fetch.events(); !slices.Equal(ev, []string{"pause " + normalTopic}) {
		t.Fatalf("events after reaching the high-water mark: %v", ev)
	}
	lim.tokens <- struct{}{} // 3 -> 2 buffered: still at the low-water mark
	lim.tokens <- struct{}{} // 2 -> 1: below it, resume
	eventually(t, "resume", func() bool {
		return slices.Equal(h.fetch.events(), []string{"pause " + normalTopic, "resume " + normalTopic})
	})
	lim.tokens <- struct{}{}
}

func TestRevokeDropsBufferedRecordsAndCommits(t *testing.T) {
	lim := &fakeLimiter{tokens: make(chan struct{})}
	h := newHarness(t, lim, Options{Concurrency: 1})
	h.sender.fn = func(provider.Message) (provider.Result, error) {
		time.Sleep(50 * time.Millisecond) // in flight while the revoke happens
		return provider.Result{MessageID: "m"}, nil
	}
	inFlight := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	buffered := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	other := h.store.add(domain.PriorityLow, domain.StatusQueued, 0)
	h.feed(t, inFlight, 0)
	h.feed(t, buffered, 0)
	h.feed(t, other, 3)
	lim.tokens <- struct{}{} // starts inFlight (oldest normal)
	eventually(t, "send started", func() bool { return len(h.sender.sent()) == 1 })

	h.w.revoke(context.Background(), map[string][]int32{normalTopic: {0}})
	// The revoke waited for the in-flight send and committed past it, but not past the dropped
	// record, which the new owner reads again.
	if got := h.store.outcome(inFlight.ID); got != "sent m" {
		t.Errorf("in-flight record: %q", got)
	}
	if got := h.committedAt(normalTopic, 0); got != 1 {
		t.Errorf("committed %d on the revoked partition, want 1", got)
	}
	lim.tokens <- struct{}{}
	lowTopic := queue.Topic(domain.ChannelSMS, domain.PriorityLow)
	eventually(t, "other partition delivered", func() bool { return h.committedAt(lowTopic, 3) == 1 })
	if h.store.outcome(buffered.ID) != "" {
		t.Error("a dropped record of a revoked partition was sent")
	}
}

func TestLimiterUnavailableSendsNothing(t *testing.T) {
	lim := &fakeLimiter{err: errors.New("redis: connection refused")}
	h := newHarness(t, lim, Options{})
	h.feed(t, h.store.add(domain.PriorityHigh, domain.StatusQueued, 0), 0)
	time.Sleep(100 * time.Millisecond)
	if len(h.sender.sent()) != 0 {
		t.Error("sent without a rate-limit token")
	}
}

func TestShutdownFinishesInFlightSends(t *testing.T) {
	h := newHarness(t, nil, Options{})
	release := make(chan struct{})
	h.sender.fn = func(provider.Message) (provider.Result, error) {
		<-release
		return provider.Result{MessageID: "m"}, nil
	}
	n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	h.feed(t, n, 0)
	eventually(t, "send started", func() bool { return len(h.sender.sent()) == 1 })

	stopped := make(chan struct{})
	go func() { h.stop(); close(stopped) }()
	time.Sleep(30 * time.Millisecond)
	close(release)
	<-stopped
	if h.store.outcome(n.ID) != "sent m" || h.committedAt(normalTopic, 0) != 1 {
		t.Errorf("after shutdown: outcome %q, committed %d", h.store.outcome(n.ID), h.committedAt(normalTopic, 0))
	}
}

// Package worker consumes one channel's work topics and delivers notifications (ADR-0002,
// ADR-0006):
//
//	Kafka batch fetch → route by topic → priority heaps → circuit breaker → Redis token →
//	capped-aging pick → guarded claim → provider → outcome in PostgreSQL (+ dead letter) →
//	partition watermark → offset commit
//
// One Worker serves one channel. Kafka spreads the channel's partitions over all worker replicas
// in the group workers.{channel}; the Redis limiter keeps the channel's global rate. Outcomes follow
// ADR-0005: classification, backoff with jitter, dead letters and a per-instance circuit breaker.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/logging"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/retry"
)

// Store is the persistence a worker needs. *storage.Store implements it.
type Store interface {
	Claim(ctx context.Context, id uuid.UUID, createdAt time.Time, lease time.Duration, token uuid.UUID) (domain.Notification, error)
	MarkSent(ctx context.Context, l domain.Lease, providerMessageID string) (domain.Notification, error)
	MarkRetrying(ctx context.Context, l domain.Lease, delay time.Duration, cause string) (domain.Notification, error)
	MarkFailed(ctx context.Context, l domain.Lease, cause string) (domain.Notification, error)
	Release(ctx context.Context, l domain.Lease, delay time.Duration, reason string) (domain.Notification, error)
	GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
}

// Sender delivers a message. *provider.Client implements it.
type Sender interface {
	Send(ctx context.Context, m provider.Message) (provider.Result, error)
}

// DeadLetterPublisher publishes the dead letters of failed notifications. *queue.DeadLetters
// implements it.
type DeadLetterPublisher interface {
	Publish(ctx context.Context, ns ...domain.Notification) error
}

// EventPublisher publishes the status events of rows the worker changed (ADR-0011). It must not
// block. *events.Publisher implements it.
type EventPublisher interface {
	Publish(ctx context.Context, ns ...domain.Notification)
}

type noEvents struct{}

func (noEvents) Publish(context.Context, ...domain.Notification) {}

// Limiter hands out send tokens. *ratelimit.Limiter implements it.
type Limiter interface {
	Wait(ctx context.Context, c domain.Channel) error
	Cooldown(ctx context.Context, c domain.Channel, d time.Duration) error
}

// Options configures a Worker. Zero values take the defaults from ADR-0002, ADR-0005 and
// ADR-0006.
type Options struct {
	// Concurrency is the number of sends in flight per channel. Default 20 (100 msg/s × ~200 ms).
	Concurrency int
	// HighWater pauses fetching a priority's topic when its buffer holds this many records;
	// LowWater resumes it. Defaults 50 and 25.
	HighWater, LowWater int
	// Lease is how long a claim lasts. It must cover the provider timeout and the outcome write;
	// an expired lease is reaped and the notification retried. Default 30s.
	Lease time.Duration
	// Retry is the backoff and attempt budget of failed sends. Zero fields take the ADR-0005
	// defaults.
	Retry retry.Policy
	// DeadLetters, if set, receives every notification this worker moves to failed.
	DeadLetters DeadLetterPublisher
	// Events receives every row this worker changes: claimed, sent, retrying, failed, released.
	// Optional.
	Events EventPublisher
	// CommitInterval is how often watermarks are committed. Default 1s.
	CommitInterval time.Duration
	// RevokeTimeout bounds the wait for in-flight sends when partitions are revoked. Default 10s.
	RevokeTimeout time.Duration
	// Breaker configures the channel's circuit breaker.
	Breaker BreakerOptions
	// Policy is the priority policy. Default DefaultPolicy().
	Policy *Policy
	// Metrics records delivery metrics (FR-4.1). Optional.
	Metrics *metrics.Delivery
	// Now returns the current time. Default time.Now.
	Now func() time.Time
}

func (o *Options) defaults() {
	setDefault(&o.Concurrency, 20)
	setDefault(&o.HighWater, 50)
	setDefault(&o.LowWater, 25)
	setDefault(&o.Lease, 30*time.Second)
	o.Retry = o.Retry.WithDefaults()
	o.Breaker.defaults()
	setDefault(&o.CommitInterval, time.Second)
	setDefault(&o.RevokeTimeout, 10*time.Second)
	if o.LowWater >= o.HighWater {
		o.LowWater = o.HighWater / 2
	}
	if o.Policy == nil {
		p := DefaultPolicy()
		o.Policy = &p
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Events == nil {
		o.Events = noEvents{}
	}
}

func setDefault[T int | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

// fetchMaxWait bounds how long a fetch waits for data; see ConsumerOptions.
const fetchMaxWait = 200 * time.Millisecond

// fetchControl pauses and resumes topics. *kgo.Client implements it.
type fetchControl interface {
	PauseFetchTopics(topics ...string) []string
	ResumeFetchTopics(topics ...string)
}

// committer commits offsets synchronously.
type committer func(ctx context.Context, offsets map[string]map[int32]kgo.EpochOffset) error

// Worker delivers one channel's notifications.
type Worker struct {
	channel domain.Channel
	store   Store
	sender  Sender
	limiter Limiter
	logger  *slog.Logger
	opts    Options

	mu     sync.Mutex // guards sched and paused
	sched  *scheduler
	paused map[domain.Priority]bool
	ready  chan struct{} // signalled when a record is buffered

	tracker  *tracker
	circuit  *circuit
	inFlight sync.WaitGroup
	// commitMu serialises commits with revocations: a periodic commit computed before a revoke
	// must not reach the broker after the revoke's commit and rewind the partition's offset.
	commitMu sync.Mutex

	fetch  fetchControl
	commit committer
}

// New returns a worker for channel c.
func New(c domain.Channel, store Store, sender Sender, limiter Limiter, logger *slog.Logger, opts Options) *Worker {
	opts.defaults()
	logger = logger.With("channel", string(c))
	opts.Metrics.BreakerState(c, metrics.BreakerClosed, "")
	return &Worker{
		channel: c, store: store, sender: sender, limiter: limiter, logger: logger, opts: opts,
		sched: newScheduler(*opts.Policy), paused: map[domain.Priority]bool{},
		ready: make(chan struct{}, 1), tracker: newTracker(),
		circuit: newCircuit("provider-"+string(c), opts.Breaker, logger, func(state int, to string) {
			opts.Metrics.BreakerState(c, state, to)
		}),
	}
}

// ConsumerOptions are the kgo options of the worker's Kafka client: the channel's consumer group
// on its three topics, the range balancer so every replica owns partitions of every priority,
// manual watermark commits, and rebalance callbacks bound to this worker.
func (w *Worker) ConsumerOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(queue.Group(w.channel)),
		kgo.ConsumeTopics(queue.Topics(w.channel)...),
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.DisableAutoCommit(),
		// Rebalances wait until a fetched batch is buffered, so no record of a revoked partition is
		// buffered after OnPartitionsRevoked dropped that partition.
		kgo.BlockRebalanceOnPoll(),
		// A new group must not skip records published before its first member joined.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// A resumed topic is fetched only after the fetch in flight returns, and a fetch on idle
		// partitions waits up to FetchMaxWait (default 5s). With backpressure pausing and resuming
		// topics continuously, that default stalled delivery for 5s per resume (measured: ~50
		// sends every 5s instead of 100/s), so keep the long poll short.
		kgo.FetchMaxWait(fetchMaxWait),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			w.logger.Info("partitions assigned", "partitions", assigned)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			w.revoke(ctx, revoked)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			w.lose(lost)
		}),
	}
}

// Run consumes with cl, which must be built with ConsumerOptions, until ctx ends. On return all
// in-flight sends have finished and their watermarks are committed. The caller then closes cl with
// CloseAllowingRebalance, which leaves the group (a plain Close waits for AllowRebalance).
func (w *Worker) Run(ctx context.Context, cl *kgo.Client) {
	w.fetch = cl
	w.commit = func(ctx context.Context, offsets map[string]map[int32]kgo.EpochOffset) error {
		return commitSync(ctx, cl, offsets)
	}
	w.run(ctx, func(ctx context.Context) bool {
		// Bounded polls keep each batch near the high-water mark; a plain PollFetches can return
		// megabytes of claim-checks per partition.
		fetches := cl.PollRecords(ctx, w.opts.HighWater)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			cl.AllowRebalance()
			return false
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			w.logger.Error("fetch failed", "topic", topic, "partition", partition, "err", err)
		})
		fetches.EachRecord(w.enqueue)
		cl.AllowRebalance()
		return true
	})
}

// run drives the engine with poll, which buffers one batch and reports whether to continue.
func (w *Worker) run(ctx context.Context, poll func(context.Context) bool) {
	var loops sync.WaitGroup
	loops.Go(func() { w.schedule(ctx) })
	loops.Go(func() { w.commitLoop(ctx) })
	for poll(ctx) {
	}
	loops.Wait()
	w.inFlight.Wait()
	// Final commit with a fresh deadline: ctx is already cancelled.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	w.commitNow(cctx, nil)
}

// enqueue buffers a fetched record in its priority's heap.
func (w *Worker) enqueue(kr *kgo.Record) {
	part := w.tracker.add(kr)
	msg, err := queue.Decode(kr)
	if err == nil && msg.Channel != w.channel {
		err = fmt.Errorf("record of channel %s on a %s worker", msg.Channel, w.channel)
	}
	if err != nil {
		// A malformed record can never be processed; skip it so it does not block its partition.
		w.logger.Error("skipping undecodable work record", "topic", kr.Topic, "partition", kr.Partition,
			"offset", kr.Offset, "err", err)
		w.tracker.done(part, kr.Offset, false)
		return
	}

	w.mu.Lock()
	n := w.sched.push(msg.Priority, &item{
		rec:   record{kr: kr, msg: msg, correlationID: queue.CorrelationID(kr), part: part},
		dueAt: msg.DueAt,
	})
	if n >= w.opts.HighWater && !w.paused[msg.Priority] {
		w.paused[msg.Priority] = true
		w.fetch.PauseFetchTopics(kr.Topic)
	}
	w.opts.Metrics.Buffered(w.channel, msg.Priority, n)
	w.mu.Unlock()

	select {
	case w.ready <- struct{}{}:
	default:
	}
}

// resumeIfDrained resumes fetching a paused priority whose buffer fell below the low-water mark.
// It is called whenever records leave a buffer, so it also updates the buffer gauge. Callers hold
// w.mu.
func (w *Worker) resumeIfDrained(pr domain.Priority) {
	w.opts.Metrics.Buffered(w.channel, pr, w.sched.len(pr))
	if w.paused[pr] && w.sched.len(pr) < w.opts.LowWater {
		w.paused[pr] = false
		w.fetch.ResumeFetchTopics(queue.Topic(w.channel, pr))
	}
}

// schedule hands buffered records to senders: it waits for a free sender, then for the circuit
// breaker to admit a send, then for a rate-limit token, and only then picks, so the pick sees
// every record that arrived meanwhile (ADR-0006). While the breaker is open nothing is picked:
// records stay buffered, the full buffers pause fetching, and the watermark holds, so the backlog
// waits safely in Kafka without burning attempts (ADR-0005).
func (w *Worker) schedule(ctx context.Context) {
	slots := make(chan struct{}, w.opts.Concurrency)
	for w.waitForRecords(ctx) {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		adm, err := w.circuit.allow()
		if err != nil {
			<-slots
			sleep(ctx, breakerPoll)
			continue
		}
		err = w.limiter.Wait(ctx, w.channel)
		granted := time.Now() // before the lock, so contention does not shift the send offset
		if err != nil {
			adm.done(errNotAttempted)
			<-slots
			if ctx.Err() != nil {
				return
			}
			// Fail closed: never send without a token (ADR-0006). Readiness reports Redis down.
			w.logger.Error("rate limiter unavailable; sending paused", "err", err)
			sleep(ctx, time.Second)
			continue
		}

		w.mu.Lock()
		it, pr := w.sched.pick(w.opts.Now())
		if it != nil {
			w.resumeIfDrained(pr)
		}
		w.mu.Unlock()
		if it == nil { // its partition was revoked while waiting for the token
			adm.done(errNotAttempted)
			<-slots
			continue
		}

		it.rec.tokenAt = granted
		w.opts.Metrics.Picked(w.channel, pr, it.rec.tokenAt.Sub(it.dueAt))
		w.tracker.start(it.rec.part)
		w.inFlight.Add(1)
		go func(r record) {
			defer w.inFlight.Done()
			defer func() { <-slots }()
			if w.process(ctx, r, adm) {
				w.tracker.done(r.part, r.kr.Offset, true)
			} else {
				w.tracker.abandon(r.part)
			}
		}(it.rec)
	}
}

// waitForRecords blocks until a record is buffered. It returns false when ctx ends.
func (w *Worker) waitForRecords(ctx context.Context) bool {
	for {
		w.mu.Lock()
		empty := w.sched.empty()
		w.mu.Unlock()
		if !empty {
			return true
		}
		select {
		case <-w.ready:
		case <-ctx.Done():
			return false
		}
	}
}

// commitLoop commits moved watermarks every CommitInterval.
func (w *Worker) commitLoop(ctx context.Context) {
	t := time.NewTicker(w.opts.CommitInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.commitNow(ctx, nil)
		}
	}
}

// commitNow commits the watermarks of the given partitions (all when nil) that moved.
func (w *Worker) commitNow(ctx context.Context, only map[string][]int32) {
	w.commitMu.Lock()
	defer w.commitMu.Unlock()
	w.commitLocked(ctx, only)
}

// commitLocked is commitNow for callers holding commitMu.
func (w *Worker) commitLocked(ctx context.Context, only map[string][]int32) {
	offsets := w.tracker.committable(only)
	if len(offsets) == 0 {
		return
	}
	if err := w.commit(ctx, offsets); err != nil {
		// Not fatal: the watermark is retried next time; at worst finished records are re-read and
		// their claims rejected as duplicates.
		w.logger.Warn("offset commit failed", "err", err)
		return
	}
	w.tracker.committed(offsets)
}

// revoke implements the rebalance handling of ADR-0002: drop the revoked partitions' unstarted
// records (the new owner re-reads them), wait a bounded time for their in-flight sends, commit
// their watermarks and forget them.
func (w *Worker) revoke(ctx context.Context, revoked map[string][]int32) {
	if len(revoked) == 0 {
		return
	}
	w.tracker.revoking(revoked)
	dropped := w.drop(revoked)
	deadline := time.Now().Add(w.opts.RevokeTimeout)
	for w.tracker.inFlight(revoked) > 0 && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if n := w.tracker.inFlight(revoked); n > 0 {
		w.logger.Warn("partitions revoked with sends still in flight", "in_flight", n)
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	w.commitMu.Lock()
	w.commitLocked(cctx, revoked)
	w.tracker.detach(revoked)
	w.commitMu.Unlock()
	w.logger.Info("partitions revoked", "partitions", revoked, "dropped_buffered", dropped)
}

// lose handles partitions taken away without a clean revoke (e.g. session expiry): their state
// is dropped without committing, since another member may already own them.
func (w *Worker) lose(lost map[string][]int32) {
	dropped := w.drop(lost)
	w.tracker.detach(lost)
	w.logger.Warn("partitions lost", "partitions", lost, "dropped_buffered", dropped)
}

// drop removes the buffered records of the given partitions and returns how many there were.
func (w *Worker) drop(parts map[string][]int32) int {
	in := func(it *item) bool {
		for _, p := range parts[it.rec.kr.Topic] {
			if p == it.rec.kr.Partition {
				return true
			}
		}
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	removed := w.sched.remove(in)
	for _, pr := range domain.Priorities {
		w.resumeIfDrained(pr)
	}
	return len(removed)
}

// commitSync commits offsets and reports the first per-partition error.
func commitSync(ctx context.Context, cl *kgo.Client, offsets map[string]map[int32]kgo.EpochOffset) error {
	var commitErr error
	cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil && commitErr == nil {
					commitErr = fmt.Errorf("commit %s/%d: %w", t.Topic, p.Partition, err)
				}
			}
		}
	})
	return commitErr
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// withCorrelation returns ctx carrying the record's correlation ID for logs.
func withCorrelation(ctx context.Context, r record) context.Context {
	if r.correlationID == "" {
		return ctx
	}
	return logging.WithCorrelationID(ctx, r.correlationID)
}

var errStale = errors.New("stale work record")

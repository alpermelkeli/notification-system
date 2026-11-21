package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// fakeOffsets hands out Kafka positions on partition 0 in publish order, like a single-partition
// topic would.
var fakeOffsets atomic.Int64

func positions(ns []domain.Notification) []KafkaPosition {
	out := make([]KafkaPosition, len(ns))
	for i := range ns {
		out[i] = KafkaPosition{Partition: 0, Offset: fakeOffsets.Add(1) - 1}
	}
	return out
}

// dispatchAll publishes every due row and returns them.
func dispatchAll(t *testing.T, s *Store) []domain.Notification {
	t.Helper()
	var published []domain.Notification
	if _, err := s.DispatchDue(context.Background(), 1000, func(_ context.Context, ns []domain.Notification) ([]KafkaPosition, error) {
		published = append(published, ns...)
		return positions(ns), nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	return published
}

func create(t *testing.T, s *Store) domain.Notification {
	t.Helper()
	n, _, err := s.CreateNotification(context.Background(), sms("hi"), Origin{})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// claim claims n and returns its lease.
func claim(t *testing.T, s *Store, n domain.Notification, lease time.Duration) domain.Lease {
	t.Helper()
	c, err := s.Claim(context.Background(), n.ID, n.CreatedAt, lease, uuid.New())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	l, ok := c.Lease()
	if !ok {
		t.Fatalf("claimed row has no lease: %+v", c)
	}
	return l
}

func TestDeliveryLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)

	published := dispatchAll(t, s)
	if len(published) != 1 || published[0].ID != n.ID {
		t.Fatalf("published %d rows, want the new one", len(published))
	}
	if got, _ := s.GetNotification(ctx, n.ID); got.Status != domain.StatusQueued || got.QueuedAt == nil {
		t.Fatalf("after dispatch: status=%s queued_at=%v", got.Status, got.QueuedAt)
	}

	claimed, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	lease, ok := claimed.Lease()
	if !ok || claimed.Attempts != 1 || claimed.LeaseUntil == nil || claimed.Content != "hi" {
		t.Fatalf("claim returned %+v", claimed)
	}

	retrying, err := s.MarkRetrying(ctx, lease, 0, "provider returned 503") // due again at once
	if err != nil {
		t.Fatal(err)
	}
	if retrying.LeaseToken != nil || retrying.LeaseUntil != nil || retrying.LastError == nil {
		t.Fatalf("retrying row still holds a lease or lost the error: %+v", retrying)
	}
	if again := dispatchAll(t, s); len(again) != 1 {
		t.Fatalf("a due retry should be re-published, got %d", len(again))
	}
	lease = claim(t, s, n, time.Minute)
	sent, err := s.MarkSent(ctx, lease, "msg-123")
	if err != nil {
		t.Fatal(err)
	}
	if sent.Status != domain.StatusSent || sent.Attempts != 2 || sent.SentAt == nil ||
		sent.ProviderMessageID == nil || *sent.ProviderMessageID != "msg-123" || sent.LastError != nil || sent.LeaseUntil != nil {
		t.Fatalf("sent row: %+v", sent)
	}

	hist, err := s.History(ctx, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []domain.Status{
		domain.StatusPending, domain.StatusQueued, domain.StatusProcessing, domain.StatusRetrying,
		domain.StatusQueued, domain.StatusProcessing, domain.StatusSent,
	}
	if len(hist) != len(want) {
		t.Fatalf("history has %d rows, want %d: %+v", len(hist), len(want), hist)
	}
	for i, h := range hist {
		if h.To != want[i] || (i > 0 && (h.From == nil || *h.From != want[i-1])) {
			t.Errorf("history[%d] = %v -> %s, want -> %s", i, h.From, h.To, want[i])
		}
	}
	if hist[3].Reason != "provider returned 503" || hist[6].Attempt != 2 {
		t.Errorf("history details: %+v / %+v", hist[3], hist[6])
	}
}

func TestDispatchSkipsRowsThatAreNotDue(t *testing.T) {
	s := newTestStore(t)
	later := time.Now().Add(time.Hour)
	in := sms("later")
	in.ScheduledAt = &later
	if _, _, err := s.CreateNotification(context.Background(), in, Origin{}); err != nil {
		t.Fatal(err)
	}
	if got := dispatchAll(t, s); len(got) != 0 {
		t.Errorf("published %d scheduled rows before they were due", len(got))
	}
}

func TestDispatchPublishFailureKeepsRowsDue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)

	boom := errors.New("kafka unavailable")
	if _, err := s.DispatchDue(ctx, 10, func(context.Context, []domain.Notification) ([]KafkaPosition, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the publish error", err)
	}
	if got, _ := s.GetNotification(ctx, n.ID); got.Status != domain.StatusPending {
		t.Fatalf("status after failed publish = %s, want pending", got.Status)
	}
	if len(dispatchAll(t, s)) != 1 {
		t.Error("the row should be published on the next iteration")
	}
}

func TestDispatchOrderAndLimit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	var ids []string
	for range 5 {
		ids = append(ids, create(t, s).ID.String())
		time.Sleep(2 * time.Millisecond) // distinct next_attempt_at
	}
	var got []string
	n, err := s.DispatchDue(ctx, 3, func(_ context.Context, ns []domain.Notification) ([]KafkaPosition, error) {
		for _, x := range ns {
			got = append(got, x.ID.String())
		}
		return positions(ns), nil
	})
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for i := range got {
		if got[i] != ids[i] {
			t.Fatalf("published %v, want the 3 oldest in FIFO order %v", got, ids[:3])
		}
	}
}

// TestConcurrentDispatchersNeverPublishTheSameRow checks FOR UPDATE SKIP LOCKED: several
// dispatchers draining the same rows publish each row exactly once.
func TestConcurrentDispatchersNeverPublishTheSameRow(t *testing.T) {
	s := newTestStore(t)
	items := make([]NewNotification, 500)
	for i := range items {
		items[i] = sms("bulk")
	}
	if _, err := s.CreateBatch(context.Background(), NewBatch{Items: items}, Origin{}); err != nil {
		t.Fatal(err)
	}

	var (
		mu    sync.Mutex
		seen  = map[string]int{}
		wg    sync.WaitGroup
		total atomic.Int64
	)
	for range 4 {
		wg.Go(func() {
			for {
				n, err := s.DispatchDue(context.Background(), 50, func(_ context.Context, ns []domain.Notification) ([]KafkaPosition, error) {
					mu.Lock()
					defer mu.Unlock()
					for _, x := range ns {
						seen[x.ID.String()]++
					}
					time.Sleep(5 * time.Millisecond) // hold the locks while "publishing"
					return positions(ns), nil
				})
				if err != nil {
					t.Errorf("dispatch: %v", err)
					return
				}
				if n == 0 {
					return
				}
				total.Add(int64(n))
			}
		})
	}
	wg.Wait()
	if total.Load() != 500 || len(seen) != 500 {
		t.Fatalf("published %d rows (%d distinct), want 500", total.Load(), len(seen))
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("%s published %d times", id, c)
		}
	}
}

// TestConcurrentClaimsHaveOneWinner models a Kafka redelivery racing the original record: only
// one worker may send.
func TestConcurrentClaimsHaveOneWinner(t *testing.T) {
	s := newTestStore(t)
	n := create(t, s)
	dispatchAll(t, s)

	var (
		wg       sync.WaitGroup
		winners  atomic.Int64
		rejected atomic.Int64
	)
	for range 8 {
		wg.Go(func() {
			_, err := s.Claim(context.Background(), n.ID, n.CreatedAt, time.Minute, uuid.New())
			switch {
			case err == nil:
				winners.Add(1)
			case errors.Is(err, domain.ErrInvalidTransition):
				rejected.Add(1)
			default:
				t.Errorf("claim: %v", err)
			}
		})
	}
	wg.Wait()
	if winners.Load() != 1 || rejected.Load() != 7 {
		t.Fatalf("winners=%d rejected=%d, want 1 and 7", winners.Load(), rejected.Load())
	}
}

func TestCancel(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	pending := create(t, s)
	got, err := s.Cancel(ctx, pending.ID)
	if err != nil || got.Status != domain.StatusCancelled {
		t.Fatalf("cancel pending: %s, %v", got.Status, err)
	}

	// Cancelling a queued notification makes the worker's claim fail (ADR-0004).
	queued := create(t, s)
	dispatchAll(t, s)
	if _, err := s.Cancel(ctx, queued.ID); err != nil {
		t.Fatalf("cancel queued: %v", err)
	}
	if _, err := s.Claim(ctx, queued.ID, queued.CreatedAt, time.Minute, uuid.New()); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("claim after cancel: err = %v, want ErrInvalidTransition", err)
	}

	// Terminal and in-flight notifications cannot be cancelled.
	var te *domain.TransitionError
	if _, err := s.Cancel(ctx, pending.ID); !errors.As(err, &te) || te.Current != domain.StatusCancelled {
		t.Fatalf("cancel twice: err = %v", err)
	}
	inFlight := create(t, s)
	dispatchAll(t, s)
	claim(t, s, inFlight, time.Minute)
	if _, err := s.Cancel(ctx, inFlight.ID); !errors.As(err, &te) || te.Current != domain.StatusProcessing {
		t.Fatalf("cancel processing: err = %v", err)
	}

	if _, err := s.Cancel(ctx, domain.NewIDAt(time.Now())); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cancel unknown: err = %v, want ErrNotFound", err)
	}
}

func TestFailReplayAndRelease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)
	dispatchAll(t, s)
	lease := claim(t, s, n, time.Minute)

	// The circuit breaker releases a claimed record without burning the attempt.
	released, err := s.Release(ctx, lease, 0, "circuit open")
	if err != nil || released.Status != domain.StatusRetrying || released.Attempts != 0 {
		t.Fatalf("release: status=%s attempts=%d err=%v", released.Status, released.Attempts, err)
	}

	dispatchAll(t, s)
	failed, err := s.MarkFailed(ctx, claim(t, s, n, time.Minute), "400 invalid recipient")
	if err != nil || failed.Status != domain.StatusFailed || failed.LastError == nil {
		t.Fatalf("fail: %+v, %v", failed, err)
	}

	replayed, err := s.Replay(ctx, n.ID)
	if err != nil || replayed.Status != domain.StatusPending || replayed.Attempts != 0 || replayed.LastError != nil {
		t.Fatalf("replay: %+v, %v", replayed, err)
	}
	if _, err := s.Replay(ctx, n.ID); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("replaying a pending notification: err = %v", err)
	}
}

func TestReapExpiredLeases(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	expired, healthy := create(t, s), create(t, s)
	dispatchAll(t, s)
	stale := claim(t, s, expired, time.Millisecond)
	claim(t, s, healthy, time.Hour)
	time.Sleep(20 * time.Millisecond)

	res, err := s.ReapExpiredLeases(ctx, 5, 100)
	if err != nil || res.Retried != 1 || len(res.Failed) != 0 {
		t.Fatalf("reaped %+v, err %v; want 1 retried", res, err)
	}
	got, _ := s.GetNotification(ctx, expired.ID)
	if got.Status != domain.StatusRetrying || got.Attempts != 1 || got.LeaseUntil != nil {
		t.Errorf("reaped row: %+v", got)
	}
	if got, _ := s.GetNotification(ctx, healthy.ID); got.Status != domain.StatusProcessing {
		t.Errorf("a live lease was reaped: %s", got.Status)
	}
	// A late outcome from the crashed worker can no longer overwrite the row (ADR-0004).
	if _, err := s.MarkSent(ctx, stale, "late"); !errors.Is(err, domain.ErrLeaseLost) {
		t.Errorf("late MarkSent: err = %v, want ErrLeaseLost", err)
	}
}

func TestReapExpiredLeaseOnTheLastAttemptFails(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)
	for range 2 { // attempts 1 and 2 fail and are retried
		dispatchAll(t, s)
		if _, err := s.MarkRetrying(ctx, claim(t, s, n, time.Minute), 0, "503"); err != nil {
			t.Fatal(err)
		}
	}
	dispatchAll(t, s)
	claim(t, s, n, time.Millisecond) // attempt 3 of 3; the worker dies
	time.Sleep(20 * time.Millisecond)

	res, err := s.ReapExpiredLeases(ctx, 3, 100)
	if err != nil || res.Retried != 0 || len(res.Failed) != 1 {
		t.Fatalf("reaped %+v, err %v; want 1 failed", res, err)
	}
	failed := res.Failed[0]
	if failed.ID != n.ID || failed.Status != domain.StatusFailed || failed.Attempts != 3 || failed.LeaseToken != nil ||
		failed.LastError == nil || *failed.LastError != "lease expired on the last attempt" {
		t.Errorf("failed row = %+v", failed)
	}
	hist, err := s.History(ctx, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	if last := hist[len(hist)-1]; last.From == nil || *last.From != domain.StatusProcessing || last.To != domain.StatusFailed ||
		last.Attempt != 3 {
		t.Errorf("last history entry = %+v", last)
	}
	// Nothing is dispatched again.
	if published := dispatchAll(t, s); len(published) != 0 {
		t.Errorf("a failed row was dispatched again: %d", len(published))
	}
}

// TestStaleLeaseCannotOverwriteANewerClaim is the ABA case: worker A stalls past its lease, the
// row is reaped, re-dispatched and claimed by worker B, so it is processing again. A's late
// outcome must not apply to B's claim.
func TestStaleLeaseCannotOverwriteANewerClaim(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)
	dispatchAll(t, s)
	leaseA := claim(t, s, n, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if res, err := s.ReapExpiredLeases(ctx, 5, 10); err != nil || res.Retried != 1 {
		t.Fatalf("reap: %+v, %v", res, err)
	}
	dispatchAll(t, s)
	leaseB := claim(t, s, n, time.Minute)

	for name, call := range map[string]func() error{
		"sent":     func() error { _, err := s.MarkSent(ctx, leaseA, "dup"); return err },
		"retrying": func() error { _, err := s.MarkRetrying(ctx, leaseA, time.Second, "503"); return err },
		"failed":   func() error { _, err := s.MarkFailed(ctx, leaseA, "400"); return err },
		"release":  func() error { _, err := s.Release(ctx, leaseA, 0, "circuit open"); return err },
	} {
		if err := call(); !errors.Is(err, domain.ErrLeaseLost) {
			t.Errorf("late %s from A: err = %v, want ErrLeaseLost", name, err)
		}
	}
	sent, err := s.MarkSent(ctx, leaseB, "msg-b")
	if err != nil || sent.Attempts != 2 || *sent.ProviderMessageID != "msg-b" {
		t.Fatalf("B's outcome: %+v, %v", sent, err)
	}
}

// position reads the stored Kafka position of a notification.
func position(t *testing.T, n domain.Notification) (*int32, *int64) {
	t.Helper()
	var (
		p *int32
		o *int64
	)
	if err := testPool.QueryRow(context.Background(),
		`SELECT kafka_partition, kafka_offset FROM notifications WHERE id = $1 AND created_at = $2`,
		n.ID, n.CreatedAt).Scan(&p, &o); err != nil {
		t.Fatal(err)
	}
	return p, o
}

func TestDispatchStoresKafkaPositions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	a, b := create(t, s), create(t, s)
	if _, err := s.DispatchDue(ctx, 10, func(_ context.Context, ns []domain.Notification) ([]KafkaPosition, error) {
		out := make([]KafkaPosition, len(ns))
		for i, x := range ns {
			out[i] = KafkaPosition{Partition: 2, Offset: 40}
			if x.ID == b.ID {
				out[i] = KafkaPosition{Partition: 5, Offset: 41}
			}
		}
		return out, nil
	}); err != nil {
		t.Fatal(err)
	}
	if p, o := position(t, a); p == nil || *p != 2 || *o != 40 {
		t.Errorf("a: partition=%v offset=%v, want 2/40", p, o)
	}
	if p, o := position(t, b); p == nil || *p != 5 || *o != 41 {
		t.Errorf("b: partition=%v offset=%v, want 5/41", p, o)
	}

	// A publisher that loses track of records must not mark anything queued.
	create(t, s)
	if _, err := s.DispatchDue(ctx, 10, func(context.Context, []domain.Notification) ([]KafkaPosition, error) {
		return nil, nil
	}); err == nil {
		t.Fatal("dispatch accepted a publish result without positions")
	}
}

func TestRequeueConsumed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	in := func(p domain.Priority) domain.Notification {
		req := sms("x")
		req.Priority = p
		n, _, err := s.CreateNotification(ctx, req, Origin{})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	lost, waiting, otherPartition, fresh, otherPriority := in(domain.PriorityNormal), in(domain.PriorityNormal),
		in(domain.PriorityNormal), in(domain.PriorityNormal), in(domain.PriorityHigh)
	at := map[uuid.UUID]KafkaPosition{
		lost.ID:           {Partition: 1, Offset: 10}, // behind the committed offset 11: consumed
		waiting.ID:        {Partition: 1, Offset: 11}, // the next record the group reads: backlog
		otherPartition.ID: {Partition: 2, Offset: 10}, // partition 2 has no committed offset
		fresh.ID:          {Partition: 1, Offset: 5},  // consumed, but queued within the grace period
		otherPriority.ID:  {Partition: 1, Offset: 3},  // high topic: its committed offset is 0
	}
	if _, err := s.DispatchDue(ctx, 10, func(_ context.Context, ns []domain.Notification) ([]KafkaPosition, error) {
		out := make([]KafkaPosition, len(ns))
		for i, x := range ns {
			out[i] = at[x.ID]
		}
		return out, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE notifications SET queued_at = now() - interval '5 minutes' WHERE id <> $1`, fresh.ID); err != nil {
		t.Fatal(err)
	}

	committed := []CommittedOffset{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Partition: 1, Offset: 11},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Partition: 1, Offset: 0},
		{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Partition: 2, Offset: 99},
	}
	n, err := s.RequeueConsumed(ctx, committed, time.Minute, 100)
	if err != nil || n != 1 {
		t.Fatalf("requeued %d, err %v; want 1", n, err)
	}
	got, _ := s.GetNotification(ctx, lost.ID)
	if got.Status != domain.StatusPending || got.QueuedAt != nil {
		t.Errorf("lost row: %+v", got)
	}
	if p, o := position(t, lost); p != nil || o != nil {
		t.Errorf("requeued row keeps its old position %v/%v", p, o)
	}
	for name, x := range map[string]domain.Notification{
		"waiting": waiting, "other partition": otherPartition, "fresh": fresh, "other priority": otherPriority,
	} {
		if got, _ := s.GetNotification(ctx, x.ID); got.Status != domain.StatusQueued {
			t.Errorf("%s row was requeued: %s", name, got.Status)
		}
	}
	hist, err := s.History(ctx, lost.ID)
	if err != nil || hist[len(hist)-1].Reason != "kafka record consumed without claim" {
		t.Errorf("history = %+v, %v", hist, err)
	}

	// The requeued row is published again with a new position.
	if again := dispatchAll(t, s); len(again) != 1 || again[0].ID != lost.ID {
		t.Fatalf("re-dispatched %d rows", len(again))
	}
	if n, err := s.RequeueConsumed(ctx, nil, 0, 100); err != nil || n != 0 {
		t.Errorf("no committed offsets: %d, %v", n, err)
	}
}

func TestRequeueStuck(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	stuck, fresh := create(t, s), create(t, s)
	dispatchAll(t, s)
	if _, err := testPool.Exec(ctx, `UPDATE notifications SET queued_at = now() - interval '1 hour' WHERE id = $1`, stuck.ID); err != nil {
		t.Fatal(err)
	}
	n, err := s.RequeueStuck(ctx, 10*time.Minute, 100)
	if err != nil || n != 1 {
		t.Fatalf("requeued %d, err %v; want 1", n, err)
	}
	if got, _ := s.GetNotification(ctx, stuck.ID); got.Status != domain.StatusPending || got.QueuedAt != nil {
		t.Errorf("stuck row: %+v", got)
	}
	if got, _ := s.GetNotification(ctx, fresh.ID); got.Status != domain.StatusQueued {
		t.Errorf("a recently queued row was requeued: %s", got.Status)
	}
}

// TestCancelDuringDispatchWaitsAndWins checks that a cancel issued while the dispatcher holds the
// row lock waits for the dispatch to commit and then cancels the queued row, so the worker's
// claim is rejected.
func TestCancelDuringDispatchWaitsAndWins(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)

	cancelled := make(chan error, 1)
	if _, err := s.DispatchDue(ctx, 10, func(_ context.Context, ns []domain.Notification) ([]KafkaPosition, error) {
		go func() { _, err := s.Cancel(ctx, n.ID); cancelled <- err }()
		select {
		case err := <-cancelled:
			return nil, fmt.Errorf("cancel did not wait for the dispatcher's lock: %w", err)
		case <-time.After(100 * time.Millisecond):
			return positions(ns), nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-cancelled; err != nil {
		t.Fatalf("cancel after dispatch: %v", err)
	}
	if _, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute, uuid.New()); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("claim after cancel: err = %v", err)
	}
	hist, err := s.History(ctx, n.ID)
	if err != nil || len(hist) != 3 || hist[1].To != domain.StatusQueued || hist[2].To != domain.StatusCancelled {
		t.Errorf("history = %+v, %v", hist, err)
	}
}

func TestTransitionValidation(t *testing.T) {
	bad := transition{from: []domain.Status{domain.StatusSent}, to: domain.StatusPending}
	if err := bad.validate(); err == nil {
		t.Error("sent -> pending must be rejected before any SQL runs")
	}
}

func TestTruncate(t *testing.T) {
	long := make([]byte, 5000)
	if got := truncate(string(long)); len(got) != 1000 {
		t.Errorf("len = %d", len(got))
	}
	if truncate("short") != "short" {
		t.Error("short strings must be unchanged")
	}
}

func TestListenDue(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	woke := make(chan struct{}, 10)
	done := make(chan error, 1)
	go func() { done <- s.ListenDue(ctx, func() { woke <- struct{}{} }) }()

	// The listener may not be registered yet; keep creating until a wake-up arrives.
	deadline := time.After(5 * time.Second)
	for got := false; !got; {
		create(t, s)
		select {
		case <-woke:
			got = true
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("no NOTIFY received for a notification due now")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("ListenDue returned %v after cancel", err)
	}
	// The connection went back to the pool without a lingering LISTEN.
	var listening int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_stat_activity WHERE query ILIKE 'LISTEN%' AND state = 'idle'`).Scan(&listening); err != nil {
		t.Fatal(err)
	}
	if listening != 0 {
		t.Errorf("%d idle connections still show a LISTEN", listening)
	}
}

// TestClaimDuringDispatchWaitsForTheCommit is the race of the ADR-0003 amendment: a worker can
// receive a record while the dispatcher has not yet committed the row as queued. The claim must
// wait for that commit and succeed, not see a pending row and give up.
func TestClaimDuringDispatchWaitsForTheCommit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)

	claimed := make(chan error, 1)
	if _, err := s.DispatchDue(ctx, 10, func(_ context.Context, ns []domain.Notification) ([]KafkaPosition, error) {
		// The record is in Kafka now; a fast worker claims it before the dispatcher commits.
		go func() { _, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute, uuid.New()); claimed <- err }()
		select {
		case err := <-claimed:
			return nil, fmt.Errorf("claim did not wait for the dispatcher's commit: %w", err)
		case <-time.After(300 * time.Millisecond):
			return positions(ns), nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-claimed; err != nil {
		t.Fatalf("claim after the dispatch commit: %v", err)
	}
	got, _ := s.GetNotification(ctx, n.ID)
	if got.Status != domain.StatusProcessing || got.Attempts != 1 {
		t.Errorf("after claim: %+v", got)
	}
	hist, _ := s.History(ctx, n.ID)
	if len(hist) != 3 || *hist[2].From != domain.StatusQueued || hist[2].To != domain.StatusProcessing {
		t.Errorf("history = %+v", hist)
	}
}

// TestClaimIsIdempotentPerToken: a claim retried with the same token after its answer was lost
// gets the claim back with a renewed lease, without counting a second attempt; another token is
// rejected.
func TestClaimIsIdempotentPerToken(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := create(t, s)
	dispatchAll(t, s)
	token := uuid.New()
	first, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute, token)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute, token)
	if err != nil {
		t.Fatalf("retry with the same token: %v", err)
	}
	if again.Attempts != 1 || *again.LeaseToken != token || again.LeaseUntil.Before(*first.LeaseUntil) {
		t.Errorf("retried claim = attempts %d token %v lease %v", again.Attempts, again.LeaseToken, again.LeaseUntil)
	}
	if _, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute, uuid.New()); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("claim with another token: err = %v, want ErrInvalidTransition", err)
	}
	hist, err := s.History(ctx, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	claims := 0
	for _, h := range hist {
		if h.Reason == "claimed" {
			claims++
		}
	}
	if claims != 1 {
		t.Errorf("%d claimed history entries, want 1", claims)
	}
}

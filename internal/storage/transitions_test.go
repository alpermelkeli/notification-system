package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// dispatchAll publishes every due row and returns them.
func dispatchAll(t *testing.T, s *Store) []domain.Notification {
	t.Helper()
	var published []domain.Notification
	if _, err := s.DispatchDue(context.Background(), 1000, func(_ context.Context, ns []domain.Notification) error {
		published = append(published, ns...)
		return nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	return published
}

func create(t *testing.T, s *Store) domain.Notification {
	t.Helper()
	n, _, err := s.CreateNotification(context.Background(), sms("hi"), "")
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// claim claims n and returns its lease.
func claim(t *testing.T, s *Store, n domain.Notification, lease time.Duration) domain.Lease {
	t.Helper()
	c, err := s.Claim(context.Background(), n.ID, n.CreatedAt, lease)
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

	claimed, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute)
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
	if _, _, err := s.CreateNotification(context.Background(), in, ""); err != nil {
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
	if _, err := s.DispatchDue(ctx, 10, func(context.Context, []domain.Notification) error { return boom }); !errors.Is(err, boom) {
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
	n, err := s.DispatchDue(ctx, 3, func(_ context.Context, ns []domain.Notification) error {
		for _, x := range ns {
			got = append(got, x.ID.String())
		}
		return nil
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
	if _, err := s.CreateBatch(context.Background(), NewBatch{Items: items}, ""); err != nil {
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
				n, err := s.DispatchDue(context.Background(), 50, func(_ context.Context, ns []domain.Notification) error {
					mu.Lock()
					defer mu.Unlock()
					for _, x := range ns {
						seen[x.ID.String()]++
					}
					time.Sleep(5 * time.Millisecond) // hold the locks while "publishing"
					return nil
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
			_, err := s.Claim(context.Background(), n.ID, n.CreatedAt, time.Minute)
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
	if _, err := s.Claim(ctx, queued.ID, queued.CreatedAt, time.Minute); !errors.Is(err, domain.ErrInvalidTransition) {
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

	n, err := s.ReapExpiredLeases(ctx, 100)
	if err != nil || n != 1 {
		t.Fatalf("reaped %d, err %v; want 1", n, err)
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
	if reaped, err := s.ReapExpiredLeases(ctx, 10); err != nil || reaped != 1 {
		t.Fatalf("reap: %d, %v", reaped, err)
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
	if _, err := s.DispatchDue(ctx, 10, func(context.Context, []domain.Notification) error {
		go func() { _, err := s.Cancel(ctx, n.ID); cancelled <- err }()
		select {
		case err := <-cancelled:
			return fmt.Errorf("cancel did not wait for the dispatcher's lock: %w", err)
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-cancelled; err != nil {
		t.Fatalf("cancel after dispatch: %v", err)
	}
	if _, err := s.Claim(ctx, n.ID, n.CreatedAt, time.Minute); !errors.Is(err, domain.ErrInvalidTransition) {
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

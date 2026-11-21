package dispatcher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeRelayStore returns scripted DispatchDue results and records when it was called.
type fakeRelayStore struct {
	mu      sync.Mutex
	results []dispatchResult // consumed in order; afterwards every call returns 0
	calls   []time.Time
	notify  chan func() // receives the ListenDue callback
}

type dispatchResult struct {
	n   int
	err error
}

func (f *fakeRelayStore) DispatchDue(ctx context.Context, limit int, publish storage.PublishFunc) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, time.Now())
	if len(f.results) == 0 {
		return 0, nil
	}
	r := f.results[0]
	f.results = f.results[1:]
	if r.n > 0 {
		if _, err := publish(ctx, make([]domain.Notification, r.n)); err != nil {
			return 0, err
		}
	}
	return min(r.n, limit), r.err
}

func (f *fakeRelayStore) ListenDue(ctx context.Context, notify func()) error {
	if f.notify != nil {
		f.notify <- notify
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeRelayStore) callTimes() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.calls...)
}

func noopPublish(_ context.Context, ns []domain.Notification) ([]storage.KafkaPosition, error) {
	return make([]storage.KafkaPosition, len(ns)), nil
}

func runRelay(t *testing.T, store *fakeRelayStore, opts RelayOptions, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	done := make(chan struct{})
	go func() { NewRelay(store, noopPublish, discard, opts).Run(ctx); close(done) }()
	<-done
}

func TestRelayDrainsFullBatchesWithoutWaiting(t *testing.T) {
	store := &fakeRelayStore{results: []dispatchResult{{n: 10}, {n: 10}, {n: 3}}}
	runRelay(t, store, RelayOptions{BatchSize: 10, PollInterval: time.Hour}, 200*time.Millisecond)

	calls := store.callTimes()
	if len(calls) != 3 {
		t.Fatalf("DispatchDue called %d times, want 3 (two full batches, then a partial one)", len(calls))
	}
	if gap := calls[2].Sub(calls[0]); gap > 50*time.Millisecond {
		t.Errorf("full batches should be drained back to back, took %v", gap)
	}
}

func TestRelayPollsWhenIdle(t *testing.T) {
	store := &fakeRelayStore{}
	runRelay(t, store, RelayOptions{PollInterval: 20 * time.Millisecond}, 210*time.Millisecond)
	if n := len(store.callTimes()); n < 5 || n > 12 {
		t.Errorf("DispatchDue called %d times in 210ms at a 20ms poll interval", n)
	}
}

func TestRelayWakesOnNotify(t *testing.T) {
	store := &fakeRelayStore{notify: make(chan func(), 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		NewRelay(store, noopPublish, discard, RelayOptions{PollInterval: time.Hour}).Run(ctx)
		close(done)
	}()

	notify := <-store.notify
	waitFor(t, func() bool { return len(store.callTimes()) == 1 })
	notify()
	notify() // coalesced with the first
	waitFor(t, func() bool { return len(store.callTimes()) >= 2 })
	time.Sleep(50 * time.Millisecond)
	if n := len(store.callTimes()); n > 3 {
		t.Errorf("%d dispatches after two notifications, want them coalesced", n)
	}
	cancel()
	<-done
}

func TestRelayBacksOffAfterErrors(t *testing.T) {
	boom := errors.New("kafka unavailable")
	store := &fakeRelayStore{results: []dispatchResult{{err: boom}, {err: boom}, {err: boom}}}
	runRelay(t, store, RelayOptions{PollInterval: time.Millisecond}, 600*time.Millisecond)

	calls := store.callTimes()
	if len(calls) < 3 {
		t.Fatalf("only %d calls", len(calls))
	}
	// Calls at 0, 100ms and 300ms: the delay doubles and is not shortened by the poll interval.
	if d := calls[2].Sub(calls[1]); d < 180*time.Millisecond {
		t.Errorf("second retry after %v, want about 200ms", d)
	}
}

func TestBackoff(t *testing.T) {
	b := newBackoff(time.Second, 5*time.Second)
	for i, want := range []time.Duration{1, 2, 4, 5, 5} {
		if got := b.next(); got != want*time.Second {
			t.Errorf("step %d: %v, want %v", i, got, want*time.Second)
		}
	}
	b.reset()
	if b.next() != time.Second {
		t.Error("reset must restart at min")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

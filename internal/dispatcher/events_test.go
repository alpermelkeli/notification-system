package dispatcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// countedEvents counts published status events.
type countedEvents struct {
	mu sync.Mutex
	n  int
}

func (c *countedEvents) Publish(_ context.Context, ns ...domain.Notification) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n += len(ns)
}

func (c *countedEvents) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// TestRelayPublishesQueuedRows: every row a dispatch committed as queued gets an event; a failed
// dispatch committed nothing and publishes nothing (ADR-0011).
func TestRelayPublishesQueuedRows(t *testing.T) {
	evts := &countedEvents{}
	store := &fakeRelayStore{results: []dispatchResult{{n: 3}, {err: errors.New("kafka unavailable")}, {n: 2}}}
	runRelay(t, store, RelayOptions{BatchSize: 10, PollInterval: time.Millisecond, Events: evts}, 300*time.Millisecond)
	if got := evts.count(); got != 5 {
		t.Errorf("published %d events, want 5", got)
	}
}

// TestReaperPublishesEveryRecoveredRow covers all four reaper steps.
func TestReaperPublishesEveryRecoveredRow(t *testing.T) {
	evts := &countedEvents{}
	failed := domain.Notification{ID: domain.NewIDAt(time.Now()), Status: domain.StatusFailed}
	store := &fakeReaperStore{leases: []int{2}, exhausted: [][]domain.Notification{{failed}}, consumed: []int{1}, stuck: []int{4}}
	offsets := fakeOffsets{"workers.sms": {"notifications.sms.high": {0: 5}}}
	r := NewReaper(store, offsets, nil, discard, ReaperOptions{Channels: []domain.Channel{domain.ChannelSMS}, Events: evts})
	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := evts.count(); got != 2+1+1+4 {
		t.Errorf("published %d events, want 8", got)
	}
}

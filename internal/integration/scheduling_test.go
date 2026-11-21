package integration

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// TestEngineDeliversScheduledNotificationsAtTheirTime covers scheduled delivery end to end (B-2):
// a notification scheduled in the future reaches the provider no earlier than its scheduledAt and
// soon after it (the relay's poll interval bounds the delay, since no NOTIFY announces it). A
// scheduled notification cancelled before its time is never sent, and a scheduledAt in the past
// means as soon as possible.
func TestEngineDeliversScheduledNotificationsAtTheirTime(t *testing.T) {
	if pool == nil {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	ctx := context.Background()
	store := storage.New(pool, storage.Options{})
	fp := newFakeProvider()
	srv := httptest.NewServer(fp)
	defer srv.Close()
	startEngine(t, store, srv.URL, 1, 100)

	create := func(recipient string, at time.Time) domain.Notification {
		t.Helper()
		at = at.UTC().Truncate(time.Microsecond)
		n, _, err := store.CreateNotification(ctx, storage.NewNotification{
			Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: recipient,
			Content: "scheduled", ScheduledAt: &at,
		}, storage.Origin{})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	at := time.Now().Add(2 * time.Second)
	scheduled := create("+905551110001", at)
	cancelled := create("+905551110002", at)
	past := create("+905551110003", time.Now().Add(-time.Hour))

	if _, err := store.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, "the past-scheduled notification sent", func() bool {
		return status(t, store, past).Status == domain.StatusSent
	})
	if got := status(t, store, scheduled); got.Status != domain.StatusPending || fp.callsFor(scheduled.ID.String()) != 0 {
		t.Fatalf("the future notification was touched before its time: %+v", got)
	}

	waitUntil(t, 10*time.Second, "the scheduled notification sent", func() bool {
		return status(t, store, scheduled).Status == domain.StatusSent
	})
	times := fp.timesFor(scheduled.ID.String())
	if len(times) != 1 {
		t.Fatalf("provider called %d times for the scheduled notification, want 1", len(times))
	}
	// The provider runs in this process, so its clock is the test's. PostgreSQL's clock decides
	// when the row is due; allow it a little skew.
	if early := at.Sub(times[0]); early > 100*time.Millisecond {
		t.Errorf("delivered %v before its scheduledAt", early)
	}
	if late := times[0].Sub(at); late > 2*time.Second {
		t.Errorf("delivered %v after its scheduledAt; the relay polls every 50 ms", late)
	}

	time.Sleep(300 * time.Millisecond) // give a wrongly dispatched cancel time to show up
	if fp.callsFor(cancelled.ID.String()) != 0 || status(t, store, cancelled).Status != domain.StatusCancelled {
		t.Error("the scheduled notification cancelled before its time was delivered")
	}
}

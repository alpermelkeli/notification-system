package storage

import (
	"context"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestQueueDepth(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	empty, err := s.QueueDepth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Due != 0 || empty.OldestDue != nil || len(empty.ByStatus) != 4 || empty.ByStatus[domain.StatusPending] != 0 {
		t.Errorf("empty depth = %+v", empty)
	}

	for _, c := range []string{"a", "b"} {
		if _, _, err := s.CreateNotification(ctx, sms(c), Origin{}); err != nil {
			t.Fatal(err)
		}
	}
	later := sms("later")
	at := time.Now().Add(time.Hour)
	later.ScheduledAt = &at
	if _, _, err := s.CreateNotification(ctx, later, Origin{}); err != nil {
		t.Fatal(err)
	}
	cancelled, _, err := s.CreateNotification(ctx, sms("cancelled"), Origin{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}

	d, err := s.QueueDepth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.ByStatus[domain.StatusPending] != 3 || d.ByStatus[domain.StatusQueued] != 0 || d.Due != 2 {
		t.Errorf("depth = %+v, want 3 pending of which 2 due", d)
	}
	if d.OldestDue == nil || time.Since(*d.OldestDue) < 0 || time.Since(*d.OldestDue) > time.Minute {
		t.Errorf("oldest due = %v", d.OldestDue)
	}
	if _, ok := d.ByStatus[domain.StatusCancelled]; ok {
		t.Error("finished statuses must not be counted")
	}
}

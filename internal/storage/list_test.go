package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestListNotifications(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var all []domain.Notification // oldest first
	for i := range 12 {
		in := sms("m")
		if i%3 == 0 {
			in.Channel, in.Recipient = domain.ChannelEmail, "a@example.com"
		}
		n, _, err := s.CreateNotification(ctx, in, Origin{})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, n)
		time.Sleep(2 * time.Millisecond) // distinct id timestamps
	}
	if _, err := s.Cancel(ctx, all[4].ID); err != nil {
		t.Fatal(err)
	}
	batch, err := s.CreateBatch(ctx, NewBatch{Items: []NewNotification{sms("b1"), sms("b2")}}, Origin{})
	if err != nil {
		t.Fatal(err)
	}

	email, cancelled := domain.ChannelEmail, domain.StatusCancelled
	from, to := all[2].CreatedAt, all[6].CreatedAt
	tests := []struct {
		name string
		f    ListFilter
		want []uuid.UUID
	}{
		{"channel", ListFilter{Channel: &email}, ids(all[9], all[6], all[3], all[0])},
		{"status", ListFilter{Status: &cancelled}, ids(all[4])},
		{"date range", ListFilter{CreatedFrom: &from, CreatedTo: &to}, ids(all[5], all[4], all[3], all[2])},
		{"batch", ListFilter{BatchID: &batch.Batch.ID}, ids(batch.Items[1].Notification, batch.Items[0].Notification)},
		{"cursor", ListFilter{After: &all[3].ID, Limit: 2}, ids(all[2], all[1])},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, err := s.ListNotifications(ctx, tt.f)
			if err != nil {
				t.Fatal(err)
			}
			if got := idsOf(page.Items); !equalIDs(got, tt.want) {
				t.Errorf("got %v\nwant %v", got, tt.want)
			}
		})
	}
}

func TestListPaginationVisitsEveryRowOnce(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	items := make([]NewNotification, 25)
	for i := range items {
		items[i] = sms("p")
	}
	if _, err := s.CreateBatch(ctx, NewBatch{Items: items}, Origin{}); err != nil {
		t.Fatal(err)
	}

	seen := map[uuid.UUID]bool{}
	var cursor *uuid.UUID
	pages := 0
	for {
		page, err := s.ListNotifications(ctx, ListFilter{After: cursor, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, n := range page.Items {
			if seen[n.ID] {
				t.Fatalf("%s returned twice", n.ID)
			}
			seen[n.ID] = true
		}
		if page.Next == nil {
			break
		}
		cursor = page.Next
	}
	if len(seen) != 25 || pages != 3 {
		t.Errorf("saw %d rows in %d pages, want 25 in 3", len(seen), pages)
	}
}

func TestListLimitIsClamped(t *testing.T) {
	s := newTestStore(t)
	create(t, s)
	create(t, s)
	page, err := s.ListNotifications(context.Background(), ListFilter{Limit: -5})
	if err != nil || len(page.Items) != 1 || page.Next == nil {
		t.Fatalf("items=%d next=%v err=%v; want 1 item and a cursor", len(page.Items), page.Next, err)
	}
}

func ids(ns ...domain.Notification) []uuid.UUID { return idsOf(ns) }

func idsOf(ns []domain.Notification) []uuid.UUID {
	out := make([]uuid.UUID, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}

func equalIDs(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func sms(content string) NewNotification {
	return NewNotification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Recipient: "+905551112233", Content: content,
	}
}

func key(k, body string) *IdempotencyKey {
	return &IdempotencyKey{Key: k, RequestHash: sha256.Sum256([]byte(body))}
}

func TestCreateAndGetNotification(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	n, created, err := s.CreateNotification(ctx, sms("hello"), "corr-1")
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	if n.Status != domain.StatusPending || n.Attempts != 0 || n.CorrelationID != "corr-1" {
		t.Errorf("unexpected new row: %+v", n)
	}
	if !n.CreatedAt.Equal(domain.IDTime(n.ID)) {
		t.Errorf("created_at %v does not match the id timestamp %v", n.CreatedAt, domain.IDTime(n.ID))
	}
	if time.Since(n.NextAttemptAt) > time.Minute || n.ScheduledAt != nil {
		t.Errorf("an unscheduled notification should be due now: next=%v scheduled=%v", n.NextAttemptAt, n.ScheduledAt)
	}

	got, err := s.GetNotification(ctx, n.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != n.ID || got.Content != "hello" || got.Channel != domain.ChannelSMS {
		t.Errorf("get returned %+v", got)
	}

	hist, err := s.History(ctx, n.ID)
	if err != nil || len(hist) != 1 || hist[0].From != nil || hist[0].To != domain.StatusPending {
		t.Fatalf("history = %+v, %v", hist, err)
	}
}

func TestGetUnknownNotification(t *testing.T) {
	s := newTestStore(t)
	for name, id := range map[string]string{
		"unknown v7": domain.NewIDAt(time.Now()).String(),
		"not a v7":   "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.GetNotification(context.Background(), mustUUID(t, id)); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("err = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestCreateScheduledNotification(t *testing.T) {
	s := newTestStore(t)
	at := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	in := sms("later")
	in.ScheduledAt = &at

	n, _, err := s.CreateNotification(context.Background(), in, "")
	if err != nil {
		t.Fatal(err)
	}
	if !n.NextAttemptAt.Equal(at) || n.ScheduledAt == nil || !n.ScheduledAt.Equal(at) {
		t.Errorf("next=%v scheduled=%v, want %v", n.NextAttemptAt, n.ScheduledAt, at)
	}
}

func TestCreateNotifiesDispatcherOnlyWhenDue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	conn, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannelDue); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(ctx, "UNLISTEN *") }()

	later := time.Now().Add(time.Hour)
	scheduled := sms("later")
	scheduled.ScheduledAt = &later
	if _, _, err := s.CreateNotification(ctx, scheduled, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateNotification(ctx, sms("now"), ""); err != nil {
		t.Fatal(err)
	}

	// Exactly one notification: the scheduled row must not wake the dispatcher.
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := conn.Conn().WaitForNotification(waitCtx); err != nil {
		t.Fatalf("no notification for a due row: %v", err)
	}
	waitCtx, cancel2 := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel2()
	if n, err := conn.Conn().WaitForNotification(waitCtx); err == nil {
		t.Fatalf("unexpected second notification: %+v", n)
	}
}

func TestCreateIdempotency(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := sms("otp 1234")
	in.IdempotencyKey = key("k-1", "otp 1234")
	first, created, err := s.CreateNotification(ctx, in, "")
	if err != nil || !created {
		t.Fatalf("first: created=%v err=%v", created, err)
	}

	again, created, err := s.CreateNotification(ctx, in, "")
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("replay: id=%s created=%v err=%v, want id %s and created=false", again.ID, created, err, first.ID)
	}

	changed := sms("otp 9999")
	changed.IdempotencyKey = key("k-1", "otp 9999")
	if _, _, err := s.CreateNotification(ctx, changed, ""); !errors.Is(err, domain.ErrIdempotencyKeyReused) {
		t.Fatalf("different body: err = %v, want ErrIdempotencyKeyReused", err)
	}

	if n := count(t, "notifications"); n != 1 {
		t.Errorf("%d notifications stored, want 1", n)
	}
}

func TestCreateIdempotencyConcurrent(t *testing.T) {
	s := newTestStore(t)
	in := sms("flash sale")
	in.IdempotencyKey = key("k-race", "flash sale")

	const n = 10
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[string]int{}
		created int
	)
	for range n {
		wg.Go(func() {
			got, c, err := s.CreateNotification(context.Background(), in, "")
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[got.ID.String()]++
			if c {
				created++
			}
		})
	}
	wg.Wait()
	if created != 1 || len(ids) != 1 {
		t.Errorf("created=%d distinct ids=%d, want exactly one notification", created, len(ids))
	}
}

func TestExpiredIdempotencyKeyIsTakenOver(t *testing.T) {
	s := newTestStore(t)
	s.ttl = time.Millisecond
	ctx := context.Background()
	in := sms("a")
	in.IdempotencyKey = key("k-exp", "a")

	first, _, err := s.CreateNotification(ctx, in, "")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	// After expiry the key behaves as new, even for a different body.
	other := sms("b")
	other.IdempotencyKey = key("k-exp", "b")
	second, created, err := s.CreateNotification(ctx, other, "")
	if err != nil || !created || second.ID == first.ID {
		t.Fatalf("after expiry: created=%v err=%v", created, err)
	}

	deleted, err := s.DeleteExpiredIdempotencyKeys(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if again, err := s.DeleteExpiredIdempotencyKeys(ctx, 100); err != nil || deleted+again != 1 {
		t.Errorf("deleted %d+%d expired keys, want 1 in total (err %v)", deleted, again, err)
	}
}

func TestCreateBatch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	items := make([]NewNotification, 300)
	for i := range items {
		items[i] = sms("bulk")
		items[i].Recipient = "+90555" + string(rune('0'+i%10))
	}
	items[7].Priority = domain.PriorityHigh

	res, err := s.CreateBatch(ctx, NewBatch{Items: items}, "corr-b")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || res.Batch.Total != 300 || len(res.Items) != 300 {
		t.Fatalf("created=%v total=%d items=%d", res.Created, res.Batch.Total, len(res.Items))
	}
	for i, it := range res.Items {
		n := it.Notification
		if n.BatchID == nil || *n.BatchID != res.Batch.ID || !n.CreatedAt.Equal(res.Batch.CreatedAt) {
			t.Fatalf("item %d: batch=%v created_at=%v, want %s / %v", i, n.BatchID, n.CreatedAt, res.Batch.ID, res.Batch.CreatedAt)
		}
		if i > 0 && n.ID.String() <= res.Items[i-1].Notification.ID.String() {
			t.Fatalf("item %d: ids are not in request order", i)
		}
	}
	if res.Items[7].Notification.Priority != domain.PriorityHigh {
		t.Error("item order or fields were mixed up")
	}

	b, err := s.GetBatch(ctx, res.Batch.ID)
	if err != nil || b.StatusCounts[domain.StatusPending] != 300 || b.CorrelationID != "corr-b" {
		t.Fatalf("GetBatch = %+v, %v", b, err)
	}
	if n := count(t, "notification_status_history"); n != 300 {
		t.Errorf("%d history rows, want 300", n)
	}
}

func TestCreateBatchIdempotency(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// An earlier single create whose key appears again as a batch item.
	single := sms("x")
	single.IdempotencyKey = key("item-a", "x")
	prior, _, err := s.CreateNotification(ctx, single, "")
	if err != nil {
		t.Fatal(err)
	}

	items := []NewNotification{sms("one"), single, sms("three")}
	batch := NewBatch{Items: items, IdempotencyKey: key("batch-1", "body")}
	res, err := s.CreateBatch(ctx, batch, "")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Items[1].Duplicate || res.Items[1].Notification.ID != prior.ID {
		t.Errorf("item 1 should be the earlier notification, got %+v", res.Items[1])
	}
	if res.Batch.Total != 2 || res.Items[0].Duplicate || res.Items[2].Duplicate {
		t.Errorf("total=%d, want 2 new items", res.Batch.Total)
	}

	// Replaying the batch key returns the original batch and creates nothing.
	replay, err := s.CreateBatch(ctx, batch, "")
	if err != nil || replay.Created || replay.Batch.ID != res.Batch.ID || len(replay.Items) != 2 {
		t.Fatalf("replay: created=%v id=%s items=%d err=%v", replay.Created, replay.Batch.ID, len(replay.Items), err)
	}
	if replay.Items[0].Notification.ID != res.Items[0].Notification.ID {
		t.Error("replayed items are not in request order")
	}

	// The same batch key with another body is rejected.
	batch.IdempotencyKey = key("batch-1", "other body")
	if _, err := s.CreateBatch(ctx, batch, ""); !errors.Is(err, domain.ErrIdempotencyKeyReused) {
		t.Errorf("err = %v, want ErrIdempotencyKeyReused", err)
	}
	if n := count(t, "notifications"); n != 3 {
		t.Errorf("%d notifications stored, want 3", n)
	}
}

func TestCreateBatchRejectsInvalidInput(t *testing.T) {
	s := newTestStore(t)
	withKey := func(k string) NewNotification {
		n := sms("x")
		n.IdempotencyKey = key(k, "x")
		return n
	}
	tests := map[string]NewBatch{
		"empty":                 {},
		"too large":             {Items: make([]NewNotification, MaxBatchSize+1)},
		"duplicate item keys":   {Items: []NewNotification{withKey("a"), withKey("a")}},
		"item reuses batch key": {Items: []NewNotification{withKey("a")}, IdempotencyKey: key("a", "x")},
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := s.CreateBatch(context.Background(), in, ""); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func mustUUID(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestConcurrentBatchesWithOverlappingKeysDoNotDeadlock: two batches claim the same item keys in
// opposite orders. Without sorted claims PostgreSQL aborts one of them with a deadlock.
func TestConcurrentBatchesWithOverlappingKeysDoNotDeadlock(t *testing.T) {
	s := newTestStore(t)
	item := func(k string) NewNotification {
		n := sms(k)
		n.IdempotencyKey = key(k, k)
		return n
	}
	var keysA, keysB []NewNotification
	for i := range 50 {
		k := fmt.Sprintf("k-%02d", i)
		keysA = append(keysA, item(k))
		keysB = append([]NewNotification{item(k)}, keysB...)
	}
	var wg sync.WaitGroup
	for _, items := range [][]NewNotification{keysA, keysB, keysA, keysB} {
		wg.Go(func() {
			if _, err := s.CreateBatch(context.Background(), NewBatch{Items: items}, ""); err != nil {
				t.Errorf("batch: %v", err)
			}
		})
	}
	wg.Wait()
	if n := count(t, "notifications"); n != 50 {
		t.Errorf("%d notifications, want 50 (one per key)", n)
	}
}

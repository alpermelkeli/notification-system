package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

type fakeReaperStore struct {
	leases, consumed, stuck []int // scripted results, one per call
	leaseErr                error
	committed               []storage.CommittedOffset
	grace, stuckAfter       time.Duration
	consumedCalls           int
}

func pop(xs *[]int) int {
	if len(*xs) == 0 {
		return 0
	}
	n := (*xs)[0]
	*xs = (*xs)[1:]
	return n
}

func (f *fakeReaperStore) ReapExpiredLeases(context.Context, int) (int, error) {
	return pop(&f.leases), f.leaseErr
}

func (f *fakeReaperStore) RequeueConsumed(_ context.Context, c []storage.CommittedOffset, grace time.Duration, _ int) (int, error) {
	f.committed, f.grace = c, grace
	f.consumedCalls++
	return pop(&f.consumed), nil
}

func (f *fakeReaperStore) RequeueStuck(_ context.Context, olderThan time.Duration, _ int) (int, error) {
	f.stuckAfter = olderThan
	return pop(&f.stuck), nil
}

type fakeOffsets map[string]map[string]map[int32]int64 // group -> topic -> partition -> offset

func (f fakeOffsets) CommittedOffsets(_ context.Context, group string) (map[string]map[int32]int64, error) {
	if group == "workers.email" {
		return nil, errors.New("coordinator not available")
	}
	return f[group], nil
}

func TestReaperRunOnce(t *testing.T) {
	store := &fakeReaperStore{leases: []int{2}, consumed: []int{1}, stuck: []int{0}}
	offsets := fakeOffsets{
		"workers.sms": {
			"notifications.sms.high":  {0: 5, 1: 7},
			"notifications.sms.low":   {2: 9},
			"notifications.push.high": {0: 1}, // not this group's channel: ignored
			"some.other.topic":        {0: 1},
		},
		"workers.push": {"notifications.push.normal": {4: 11}},
	}
	r := NewReaper(store, offsets, discard, ReaperOptions{Grace: 2 * time.Minute, StuckAfter: 6 * time.Hour})

	rep, err := r.RunOnce(context.Background())
	if err == nil {
		t.Error("the email group's offset error must be reported")
	}
	if rep != (ReapReport{ExpiredLeases: 2, Consumed: 1}) {
		t.Errorf("report = %+v", rep)
	}
	key := func(c storage.CommittedOffset) string {
		return fmt.Sprintf("%s.%s/%d=%d", c.Channel, c.Priority, c.Partition, c.Offset)
	}
	var got []string
	for _, c := range store.committed {
		got = append(got, key(c))
	}
	slices.Sort(got)
	want := []string{"push.normal/4=11", "sms.high/0=5", "sms.high/1=7", "sms.low/2=9"}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("committed offsets = %v, want %v", got, want)
	}
	if store.grace != 2*time.Minute || store.stuckAfter != 6*time.Hour {
		t.Errorf("grace %v, stuck after %v", store.grace, store.stuckAfter)
	}
}

func TestReaperStepsAreIndependentAndDrain(t *testing.T) {
	store := &fakeReaperStore{
		leaseErr: errors.New("db hiccup"),
		stuck:    []int{3, 3, 1}, // limit 3: two full rounds, then a partial one
	}
	r := NewReaper(store, fakeOffsets{}, discard, ReaperOptions{Limit: 3, Channels: []domain.Channel{domain.ChannelSMS}})
	rep, err := r.RunOnce(context.Background())
	if err == nil {
		t.Error("the lease step error must be reported")
	}
	if rep.Stuck != 7 {
		t.Errorf("stuck = %d, want 7 over three rounds", rep.Stuck)
	}
	if store.consumedCalls != 0 {
		t.Error("RequeueConsumed must not run without committed offsets")
	}
}

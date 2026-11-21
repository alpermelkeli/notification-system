package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

type fakeReaperStore struct {
	leases, consumed, stuck []int // scripted results, one per call
	exhausted               [][]domain.Notification
	leaseErr                error
	maxAttempts             int
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

func (f *fakeReaperStore) ReapExpiredLeases(_ context.Context, maxAttempts, _ int) (storage.ReapResult, error) {
	f.maxAttempts = maxAttempts
	res := storage.ReapResult{Retried: make([]domain.Notification, pop(&f.leases))}
	if len(f.exhausted) > 0 {
		res.Failed, f.exhausted = f.exhausted[0], f.exhausted[1:]
	}
	return res, f.leaseErr
}

type fakeDeadLetters struct {
	published []domain.Notification
	err       error
}

func (f *fakeDeadLetters) Publish(_ context.Context, ns ...domain.Notification) error {
	f.published = append(f.published, ns...)
	return f.err
}

func (f *fakeReaperStore) RequeueConsumed(_ context.Context, c []storage.CommittedOffset, grace time.Duration, _ int) ([]domain.Notification, error) {
	f.committed, f.grace = c, grace
	f.consumedCalls++
	return make([]domain.Notification, pop(&f.consumed)), nil
}

func (f *fakeReaperStore) RequeueStuck(_ context.Context, olderThan time.Duration, _ int) ([]domain.Notification, error) {
	f.stuckAfter = olderThan
	return make([]domain.Notification, pop(&f.stuck)), nil
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
	r := NewReaper(store, offsets, nil, discard, ReaperOptions{Grace: 2 * time.Minute, StuckAfter: 6 * time.Hour})

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
	r := NewReaper(store, fakeOffsets{}, nil, discard, ReaperOptions{Limit: 3, Channels: []domain.Channel{domain.ChannelSMS}})
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

func TestReaperDeadLettersExhaustedLeases(t *testing.T) {
	a, b := domain.Notification{ID: domain.NewIDAt(time.Now())}, domain.Notification{ID: domain.NewIDAt(time.Now())}
	store := &fakeReaperStore{leases: []int{1, 0}, exhausted: [][]domain.Notification{{a}, {b}}}
	dead := &fakeDeadLetters{err: errors.New("broker down")} // best effort: the run still succeeds
	reg := metrics.NewRegistry()
	r := NewReaper(store, fakeOffsets{}, dead, discard, ReaperOptions{
		Limit: 2, MaxAttempts: 3, Channels: []domain.Channel{domain.ChannelSMS}, Metrics: metrics.NewDispatch(reg),
	})
	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// Round 1 changes 2 rows (= Limit), so a second round runs.
	if rep.ExpiredLeases != 1 || rep.ExhaustedLeases != 2 {
		t.Errorf("report = %+v", rep)
	}
	if store.maxAttempts != 3 {
		t.Errorf("max attempts passed = %d", store.maxAttempts)
	}
	if len(dead.published) != 2 || dead.published[0].ID != a.ID || dead.published[1].ID != b.ID {
		t.Errorf("dead letters = %+v", dead.published)
	}
	want := `
# HELP notifier_dead_letters_total Dead letters of failed notifications, by publishing component and result (published, failed).
# TYPE notifier_dead_letters_total counter
notifier_dead_letters_total{result="failed",source="reaper"} 2
# HELP notifier_reaper_recovered_total Notifications recovered by the reaper, by reason (expired_lease, exhausted_lease, consumed, stuck).
# TYPE notifier_reaper_recovered_total counter
notifier_reaper_recovered_total{reason="exhausted_lease"} 2
notifier_reaper_recovered_total{reason="expired_lease"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"notifier_dead_letters_total", "notifier_reaper_recovered_total"); err != nil {
		t.Error(err)
	}
}

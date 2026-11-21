package dispatcher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

type fakeDepthStore struct {
	depth storage.QueueDepth
	err   error
}

func (f *fakeDepthStore) QueueDepth(context.Context) (storage.QueueDepth, error) {
	return f.depth, f.err
}

type fakeLag struct {
	lag    map[string]map[string]int64
	err    error
	groups []string
}

func (f *fakeLag) Lag(_ context.Context, groups ...string) (map[string]map[string]int64, error) {
	f.groups = groups
	return f.lag, f.err
}

func TestDepthMonitorSample(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-90 * time.Second)
	store := &fakeDepthStore{depth: storage.QueueDepth{
		ByStatus: map[domain.Status]int{domain.StatusPending: 120, domain.StatusQueued: 30, domain.StatusProcessing: 5, domain.StatusRetrying: 2},
		Due:      100, OldestDue: &oldest,
	}}
	lag := &fakeLag{lag: map[string]map[string]int64{
		"workers.sms": {"notifications.sms.high": 7, "notifications.sms.low": 0, "unrelated.topic": 99},
	}}
	reg := metrics.NewRegistry()
	d := NewDepthMonitor(store, lag, metrics.NewDispatch(reg), discard, time.Second)
	d.now = func() time.Time { return now }

	if err := d.SampleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(lag.groups, ",") != "workers.sms,workers.email,workers.push" {
		t.Errorf("groups asked = %v", lag.groups)
	}
	want := `
# HELP notifier_kafka_consumer_lag Work records not yet committed by the channel's consumer group, by topic: the workers' backlog.
# TYPE notifier_kafka_consumer_lag gauge
notifier_kafka_consumer_lag{group="workers.sms",topic="notifications.sms.high"} 7
notifier_kafka_consumer_lag{group="workers.sms",topic="notifications.sms.low"} 0
# HELP notifier_notifications Unfinished notifications in PostgreSQL by status (pending, queued, processing, retrying).
# TYPE notifier_notifications gauge
notifier_notifications{status="pending"} 120
notifier_notifications{status="processing"} 5
notifier_notifications{status="queued"} 30
notifier_notifications{status="retrying"} 2
# HELP notifier_notifications_due Pending and retrying notifications that are due now: the dispatcher's backlog.
# TYPE notifier_notifications_due gauge
notifier_notifications_due 100
# HELP notifier_oldest_due_age_seconds How long the longest-waiting due notification has waited for dispatch; 0 when none is due.
# TYPE notifier_oldest_due_age_seconds gauge
notifier_oldest_due_age_seconds 90
`
	names := []string{"notifier_kafka_consumer_lag", "notifier_notifications", "notifier_notifications_due", "notifier_oldest_due_age_seconds"}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), names...); err != nil {
		t.Error(err)
	}

	// A failing source is counted and keeps the gauges at their last value.
	store.err, lag.err = errors.New("db down"), errors.New("broker down")
	if err := d.SampleOnce(context.Background()); err == nil {
		t.Fatal("expected the sources' errors")
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), names...); err != nil {
		t.Errorf("gauges changed after a failed sample: %v", err)
	}
	if got := testutil.CollectAndCount(reg, "notifier_queue_depth_errors_total"); got != 2 {
		t.Errorf("depth error series = %d, want postgres and kafka", got)
	}
}

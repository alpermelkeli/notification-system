package integration

import (
	"context"
	"net/http/httptest"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// eventLog reads notification.events from the start, like a WebSocket hub without a group, and
// keeps the events of each notification.
type eventLog struct {
	mu   sync.Mutex
	byID map[uuid.UUID][]queue.StatusEvent
}

func readEvents(t *testing.T) *eventLog {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr), kgo.ConsumeTopics(queue.EventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	log := &eventLog{byID: map[uuid.UUID][]queue.StatusEvent{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done; cl.Close() })
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
				e, err := queue.DecodeStatusEvent(r)
				if err != nil {
					t.Errorf("undecodable event: %v", err)
					return
				}
				log.mu.Lock()
				log.byID[e.NotificationID] = append(log.byID[e.NotificationID], e)
				log.mu.Unlock()
			})
		}
	}()
	return log
}

// statuses returns the statuses of a notification's events ordered by At, as a client would.
func (l *eventLog) statuses(id uuid.UUID) []domain.Status {
	l.mu.Lock()
	es := slices.Clone(l.byID[id])
	l.mu.Unlock()
	sort.SliceStable(es, func(i, j int) bool { return es[i].At.Before(es[j].At) })
	out := make([]domain.Status, len(es))
	for i, e := range es {
		out[i] = e.Status
	}
	return out
}

// TestEngineStreamsStatusEvents: the dispatcher and the workers publish every transition to
// notification.events after committing it, so a consumer ordering by At sees the full lifecycle,
// retries included (ADR-0011). Events carry the batch id that WebSocket subscriptions use.
func TestEngineStreamsStatusEvents(t *testing.T) {
	if pool == nil {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	ctx := context.Background()
	store := storage.New(pool, storage.Options{})
	fp := newFakeProvider()
	fp.failOnce["+905552220002"] = true
	srv := httptest.NewServer(fp)
	defer srv.Close()
	log := readEvents(t)
	startEngine(t, store, srv.URL, 1, 100)

	res, err := store.CreateBatch(ctx, storage.NewBatch{Items: []storage.NewNotification{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Recipient: "+905552220001", Content: "ok"},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Recipient: "+905552220002", Content: "flaky"},
	}}, storage.Origin{CorrelationID: "corr-events"})
	if err != nil {
		t.Fatal(err)
	}
	ok, flaky := res.Items[0].Notification, res.Items[1].Notification

	want := map[uuid.UUID][]domain.Status{
		ok.ID: {domain.StatusQueued, domain.StatusProcessing, domain.StatusSent},
		flaky.ID: {domain.StatusQueued, domain.StatusProcessing, domain.StatusRetrying,
			domain.StatusQueued, domain.StatusProcessing, domain.StatusSent},
	}
	waitUntil(t, 20*time.Second, "every transition streamed", func() bool {
		for id, w := range want {
			if !slices.Equal(log.statuses(id), w) {
				return false
			}
		}
		return true
	})

	log.mu.Lock()
	defer log.mu.Unlock()
	for _, e := range log.byID[flaky.ID] {
		if e.BatchID == nil || *e.BatchID != res.Batch.ID || e.Channel != domain.ChannelSMS || e.Priority != domain.PriorityHigh {
			t.Errorf("event %+v lacks the notification's batch, channel or priority", e)
		}
		if e.Status == domain.StatusRetrying && (e.LastError == nil || e.NextAttemptAt == nil || e.Attempts != 1) {
			t.Errorf("retrying event %+v lacks its error, next attempt or attempt count", e)
		}
	}
}

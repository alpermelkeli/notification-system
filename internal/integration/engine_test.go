// Package integration runs the processing engine end to end against real PostgreSQL, Kafka and
// Redis containers, with an httptest server as the provider. It is skipped under -short.
package integration

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/dispatcher"
	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/ratelimit"
	"github.com/alpermelkeli/notification-system/internal/storage"
	"github.com/alpermelkeli/notification-system/internal/testinfra"
	"github.com/alpermelkeli/notification-system/internal/worker"
	"github.com/alpermelkeli/notification-system/migrations"
)

var (
	pool      *pgxpool.Pool
	kafkaAddr string
	redisAddr string
	quiet     = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	var (
		stops []func()
		err   error
		stop  func()
	)
	defer func() {
		for _, s := range stops {
			s()
		}
	}()
	if pool, stop, err = testinfra.Postgres(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	stops = append(stops, stop)
	if kafkaAddr, stop, err = testinfra.Kafka(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	stops = append(stops, stop)
	if redisAddr, stop, err = testinfra.Redis(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	stops = append(stops, stop)
	if err := storage.Migrate(ctx, pool, migrations.FS, quiet); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	if err := storage.New(pool, storage.Options{}).EnsurePartitions(ctx, time.Now().AddDate(0, 0, -1), 3); err != nil {
		fmt.Fprintln(os.Stderr, "partitions:", err)
		return 1
	}
	if err := createTopics(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

// createTopics creates the work topics with the partition counts of ADR-0002.
func createTopics(ctx context.Context) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, c := range domain.Channels {
		for _, p := range domain.Priorities {
			partitions := int32(6)
			if p == domain.PriorityLow {
				partitions = 3
			}
			resp, err := adm.CreateTopic(ctx, partitions, 1, nil, queue.Topic(c, p))
			if err == nil {
				err = resp.Err
			}
			if err != nil {
				return fmt.Errorf("create topic %s: %w", queue.Topic(c, p), err)
			}
		}
	}
	return nil
}

// fakeProvider is the provider: it records every request and fails the first attempt of
// recipients listed in failOnce with 503.
type fakeProvider struct {
	mu       sync.Mutex
	calls    map[string]int // Idempotency-Key -> requests
	times    []time.Time
	failOnce map[string]bool
}

func (p *fakeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct{ To string }
	_ = json.NewDecoder(r.Body).Decode(&body)
	p.mu.Lock()
	key := r.Header.Get(provider.HeaderIdempotencyKey)
	p.calls[key]++
	p.times = append(p.times, time.Now())
	fail := p.failOnce[body.To] && p.calls[key] == 1
	p.mu.Unlock()
	if fail {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, `{"messageId":"pm-%s","status":"accepted"}`, key)
}

func (p *fakeProvider) callsFor(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[key]
}

// engine runs a dispatcher (relay + reaper) and n SMS workers until the test ends.
func startEngine(t *testing.T, store *storage.Store, providerURL string, workers int, rate int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })

	producer, err := kgo.NewClient(append(dispatcher.ProducerOptions(), kgo.SeedBrokers(kafkaAddr))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	relay := dispatcher.NewRelay(store, dispatcher.NewPublisher(producer).Publish, quiet,
		dispatcher.RelayOptions{PollInterval: 50 * time.Millisecond})
	reaper := dispatcher.NewReaper(store, dispatcher.NewKafkaOffsets(kadm.NewClient(producer)), nil, quiet,
		dispatcher.ReaperOptions{Interval: 200 * time.Millisecond, Grace: 100 * time.Millisecond})
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() { reaper.Run(ctx) })

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	sender, err := provider.New(providerURL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := ratelimit.New(rdb, sender.Name(), map[domain.Channel]ratelimit.Limit{
		domain.ChannelSMS: {PerSecond: rate, Burst: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range workers {
		w := worker.New(domain.ChannelSMS, store, sender, limiter, quiet, worker.Options{
			RetryDelay: 200 * time.Millisecond, MaxAttempts: 3, Lease: 10 * time.Second,
			CommitInterval: 100 * time.Millisecond,
		})
		cl, err := kgo.NewClient(append(w.ConsumerOptions(), kgo.SeedBrokers(kafkaAddr))...)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() { w.Run(ctx, cl); cl.CloseAllowingRebalance() })
	}
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func status(t *testing.T, store *storage.Store, n domain.Notification) domain.Notification {
	t.Helper()
	got, err := store.GetNotification(context.Background(), n.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestEngineDeliversEveryNotificationOnce is the Phase 5 happy path and its main failure paths:
// rows written by the API are published, consumed by two workers sharing the group (each owning
// partitions of all three priorities), rate limited, delivered once each, retried after a 503,
// and a cancelled row is never sent.
func TestEngineDeliversEveryNotificationOnce(t *testing.T) {
	if pool == nil {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	ctx := context.Background()
	store := storage.New(pool, storage.Options{})
	fp := &fakeProvider{calls: map[string]int{}, failOnce: map[string]bool{"+905550000001": true}}
	srv := httptest.NewServer(fp)
	defer srv.Close()

	// Cancelled before any dispatcher runs: it must never reach the provider.
	cancelled, _, err := store.CreateNotification(ctx, storage.NewNotification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: "+905559999999", Content: "x",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}

	const rate = 25
	startEngine(t, store, srv.URL, 2, rate)
	waitForSpread(t, 2)

	var ns []domain.Notification
	for i := range 40 {
		recipient := fmt.Sprintf("+90555%07d", i+2)
		if i == 0 {
			recipient = "+905550000001" // fails once with 503
		}
		n, _, err := store.CreateNotification(ctx, storage.NewNotification{
			Channel: domain.ChannelSMS, Priority: domain.Priorities[i%3], Recipient: recipient, Content: "hello",
		}, "corr-e2e")
		if err != nil {
			t.Fatal(err)
		}
		ns = append(ns, n)
	}

	start := time.Now()
	waitUntil(t, 30*time.Second, "every notification sent", func() bool {
		for _, n := range ns {
			if status(t, store, n).Status != domain.StatusSent {
				return false
			}
		}
		return true
	})
	elapsed := time.Since(start)

	for i, n := range ns {
		want := 1
		if i == 0 {
			want = 2
		}
		if got := fp.callsFor(n.ID.String()); got != want {
			t.Errorf("notification %d: provider called %d times, want %d", i, got, want)
		}
		if got := status(t, store, n); *got.ProviderMessageID != "pm-"+n.ID.String() || got.Attempts != want {
			t.Errorf("notification %d: %+v", i, got)
		}
	}
	if fp.callsFor(cancelled.ID.String()) != 0 || status(t, store, cancelled).Status != domain.StatusCancelled {
		t.Error("the cancelled notification was delivered")
	}
	hist, err := store.History(ctx, ns[0].ID)
	if err != nil || len(hist) != 7 || hist[3].To != domain.StatusRetrying || hist[6].To != domain.StatusSent {
		t.Errorf("history of the retried notification = %+v, %v", hist, err)
	}

	// 41 provider calls at 25/s with burst 1 need at least 1.6 s, whatever the worker count.
	if minimum := 40 * time.Second / rate; elapsed < minimum*9/10 {
		t.Errorf("41 sends took %v; the %d/s limit needs at least %v", elapsed, rate, minimum)
	}
}

// TestReaperRecoversARecordConsumedWithoutClaim simulates a lost record: the row is queued at an
// offset the workers have already committed past. The offset rule requeues it and it is
// delivered.
func TestReaperRecoversARecordConsumedWithoutClaim(t *testing.T) {
	if pool == nil {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	ctx := context.Background()
	store := storage.New(pool, storage.Options{})
	fp := &fakeProvider{calls: map[string]int{}}
	srv := httptest.NewServer(fp)
	defer srv.Close()
	startEngine(t, store, srv.URL, 1, 100)

	// Deliver one notification so the group commits an offset on its partition.
	first, _, err := store.CreateNotification(ctx, storage.NewNotification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Recipient: "+905551110000", Content: "a",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 20*time.Second, "first delivery", func() bool { return status(t, store, first).Status == domain.StatusSent })
	var partition int32
	var offset int64
	if err := pool.QueryRow(ctx, `SELECT kafka_partition, kafka_offset FROM notifications WHERE id = $1`, first.ID).
		Scan(&partition, &offset); err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(mustClient(t))
	waitUntil(t, 10*time.Second, "offset commit", func() bool {
		got, err := dispatcher.NewKafkaOffsets(adm).CommittedOffsets(ctx, queue.Group(domain.ChannelSMS))
		return err == nil && got[queue.Topic(domain.ChannelSMS, domain.PriorityHigh)][partition] > offset
	})

	// A row that claims to be queued at that already-consumed offset, but was never claimed.
	lost, _, err := store.CreateNotification(ctx, storage.NewNotification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Recipient: "+905551110001", Content: "b",
		ScheduledAt: ptr(time.Now().Add(time.Hour)), // not due, so the relay leaves it alone
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE notifications SET status = 'queued', queued_at = now() - interval '1 minute',
		       kafka_partition = $2, kafka_offset = $3
		 WHERE id = $1`, lost.ID, partition, offset); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 20*time.Second, "lost row recovered and delivered", func() bool {
		return status(t, store, lost).Status == domain.StatusSent
	})
	hist, err := store.History(ctx, lost.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, h := range hist {
		found = found || h.Reason == "kafka record consumed without claim"
	}
	if !found || fp.callsFor(lost.ID.String()) != 1 {
		t.Errorf("history %+v, provider calls %d", hist, fp.callsFor(lost.ID.String()))
	}
}

// waitForSpread waits until the SMS group is stable with n members, and checks that the range
// assignor gave every member partitions of all three priority topics (ADR-0002), so each replica
// can apply priority locally.
func waitForSpread(t *testing.T, n int) {
	t.Helper()
	adm := kadm.NewClient(mustClient(t))
	var last string
	waitUntil(t, 30*time.Second, "a stable group with every member on every topic", func() bool {
		groups, err := adm.DescribeGroups(context.Background(), queue.Group(domain.ChannelSMS))
		if err != nil {
			last = err.Error()
			return false
		}
		g := groups[queue.Group(domain.ChannelSMS)]
		last = fmt.Sprintf("state %s, %d members", g.State, len(g.Members))
		if g.State != "Stable" || len(g.Members) != n {
			return false
		}
		for _, m := range g.Members {
			a, ok := m.Assigned.AsConsumer()
			if !ok || len(a.Topics) != len(domain.Priorities) {
				return false
			}
			for _, tp := range a.Topics {
				if len(tp.Partitions) == 0 {
					return false
				}
			}
		}
		return true
	})
	t.Logf("consumer group: %s", last)
}

func mustClient(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func ptr[T any](v T) *T { return &v }

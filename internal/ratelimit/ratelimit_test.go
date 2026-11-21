package ratelimit

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/testinfra"
)

var redisAddr string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	addr, stop, err := testinfra.Redis(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	redisAddr = addr
	code := m.Run()
	stop()
	os.Exit(code)
}

// newClient returns a client on an empty Redis.
func newClient(t *testing.T) *redis.Client {
	t.Helper()
	if redisAddr == "" {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return rdb
}

// TestLimitHoldsAcrossLimiters models several worker replicas sharing one channel: together they
// never exceed the configured rate.
func TestLimitHoldsAcrossLimiters(t *testing.T) {
	rdb := newClient(t)
	const rate, burst, tokens = 50, 5, 100
	var (
		mu    sync.Mutex
		times []time.Time
		wg    sync.WaitGroup
	)
	start := time.Now()
	for range 3 {
		l, err := New(rdb, "mock:8081", map[domain.Channel]Limit{domain.ChannelSMS: {PerSecond: rate, Burst: burst}})
		if err != nil {
			t.Fatal(err)
		}
		for range 4 {
			wg.Go(func() {
				for {
					mu.Lock()
					done := len(times) >= tokens
					mu.Unlock()
					if done {
						return
					}
					if err := l.Wait(context.Background(), domain.ChannelSMS); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					times = append(times, time.Now())
					mu.Unlock()
				}
			})
		}
	}
	wg.Wait()

	// GCRA emits one token every 1/rate after the burst, so the first `tokens` grants need at
	// least (tokens-burst)/rate.
	if elapsed, min := times[tokens-1].Sub(start), time.Duration(tokens-burst)*time.Second/rate; elapsed < min*9/10 {
		t.Errorf("%d tokens took %v, want at least %v", tokens, elapsed, min)
	}
	// No one-second window holds more than rate + burst grants.
	for i := range times {
		n := 0
		for _, u := range times[i:] {
			if u.Sub(times[i]) < time.Second {
				n++
			}
		}
		if n > rate+burst {
			t.Fatalf("%d tokens granted within one second, limit %d + burst %d", n, rate, burst)
		}
	}
}

func TestChannelsHaveSeparateBudgets(t *testing.T) {
	rdb := newClient(t)
	l, err := New(rdb, "p", map[domain.Channel]Limit{
		domain.ChannelSMS:   {PerSecond: 1, Burst: 1},
		domain.ChannelEmail: {PerSecond: 1, Burst: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx, domain.ChannelSMS); err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(ctx, domain.ChannelEmail); err != nil {
		t.Fatalf("email must not wait for sms tokens: %v", err)
	}
	if err := l.Wait(ctx, domain.ChannelSMS); !errors.Is(err, context.DeadlineExceeded) || IsUnavailable(err) {
		t.Fatalf("second sms token within a second: err = %v, want the context deadline", err)
	}
	if err := l.Wait(ctx, domain.ChannelPush); err == nil {
		t.Error("a channel without a limit must be rejected")
	}
}

func TestCooldownPausesEveryLimiter(t *testing.T) {
	rdb := newClient(t)
	limits := map[domain.Channel]Limit{domain.ChannelPush: {PerSecond: 1000, Burst: 10}}
	a, _ := New(rdb, "p", limits)
	b, _ := New(rdb, "p", limits)
	ctx := context.Background()

	if err := a.Cooldown(ctx, domain.ChannelPush, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// A shorter cool-down never shortens the one in place.
	if err := a.Cooldown(ctx, domain.ChannelPush, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := b.Wait(ctx, domain.ChannelPush); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited < 250*time.Millisecond {
		t.Errorf("waited %v during a 300ms cool-down set by another worker", waited)
	}
	if ttl := rdb.PTTL(ctx, "ratelimit:push:p:cooldown").Val(); ttl > 0 {
		t.Errorf("cool-down still set: %v", ttl)
	}
	if err := a.Cooldown(ctx, domain.ChannelPush, time.Hour); err != nil {
		t.Fatal(err)
	}
	if ttl := rdb.PTTL(ctx, "ratelimit:push:p:cooldown").Val(); ttl > MaxCooldown {
		t.Errorf("cool-down %v exceeds the cap", ttl)
	}
}

func TestRedisDownFailsClosed(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	defer func() { _ = rdb.Close() }()
	l, err := New(rdb, "p", map[domain.Channel]Limit{domain.ChannelSMS: {PerSecond: 10, Burst: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(context.Background(), domain.ChannelSMS); !IsUnavailable(err) {
		t.Fatalf("err = %v, want a Redis error", err)
	}
}

func TestNewRejectsInvalidLimits(t *testing.T) {
	for _, lim := range []Limit{{0, 1}, {1, 0}, {-1, 5}} {
		if _, err := New(nil, "p", map[domain.Channel]Limit{domain.ChannelSMS: lim}); err == nil {
			t.Errorf("New accepted %+v", lim)
		}
	}
}

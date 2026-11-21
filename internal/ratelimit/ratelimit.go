// Package ratelimit enforces the per-channel send rate across every worker replica (FR-2.2,
// ADR-0006). Tokens come from a GCRA bucket in Redis (go-redis/redis_rate: one atomic Lua script
// per check, one key per channel and provider), so the limit holds whatever the number of
// workers. A shared cool-down key lets one worker that is throttled by the provider (429) pause
// the whole fleet for that channel.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// Limit is the rate of one channel.
type Limit struct {
	// PerSecond is the sustained rate. It should leave headroom below the provider's own limit.
	PerSecond int
	// Burst is how many tokens may be taken at once after an idle period.
	Burst int
}

// MaxCooldown caps a provider's Retry-After, so a bogus value cannot stall a channel for long.
const MaxCooldown = time.Minute

// Limiter hands out send tokens. It is safe for concurrent use.
type Limiter struct {
	rdb      redis.UniversalClient
	gcra     *redis_rate.Limiter
	provider string
	limits   map[domain.Channel]redis_rate.Limit
}

// New returns a limiter for the provider endpoint named provider. Every channel that will ask for
// tokens must have a limit.
func New(rdb redis.UniversalClient, provider string, limits map[domain.Channel]Limit) (*Limiter, error) {
	l := &Limiter{rdb: rdb, gcra: redis_rate.NewLimiter(rdb), provider: provider, limits: map[domain.Channel]redis_rate.Limit{}}
	for c, lim := range limits {
		if lim.PerSecond <= 0 || lim.Burst <= 0 {
			return nil, fmt.Errorf("rate limit of %s must be positive, got %d/s burst %d", c, lim.PerSecond, lim.Burst)
		}
		l.limits[c] = redis_rate.Limit{Rate: lim.PerSecond, Burst: lim.Burst, Period: time.Second}
	}
	return l, nil
}

// Key is the GCRA key of a channel. redis_rate stores it with its own "rate:" prefix.
func (l *Limiter) Key(c domain.Channel) string { return "ratelimit:" + string(c) + ":" + l.provider }

func (l *Limiter) cooldownKey(c domain.Channel) string { return l.Key(c) + ":cooldown" }

// Wait blocks until a token for channel c is granted or ctx ends. It first honours a fleet-wide
// cool-down. A Redis error is returned at once: the caller must not send without a token (fail
// closed, ADR-0006).
func (l *Limiter) Wait(ctx context.Context, c domain.Channel) error {
	limit, ok := l.limits[c]
	if !ok {
		return fmt.Errorf("no rate limit configured for channel %s", c)
	}
	for {
		ttl, err := l.rdb.PTTL(ctx, l.cooldownKey(c)).Result()
		if err != nil {
			return fmt.Errorf("read rate-limit cool-down: %w", err)
		}
		if ttl > 0 {
			if err := sleep(ctx, ttl); err != nil {
				return err
			}
			continue
		}
		res, err := l.gcra.Allow(ctx, l.Key(c), limit)
		if err != nil {
			return fmt.Errorf("take rate-limit token: %w", err)
		}
		if res.Allowed > 0 {
			return nil
		}
		if err := sleep(ctx, max(res.RetryAfter, time.Millisecond)); err != nil {
			return err
		}
	}
}

// Cooldown pauses channel c for every worker for d (capped at MaxCooldown), e.g. after the
// provider answered 429 with Retry-After. A longer cool-down already in place is kept.
func (l *Limiter) Cooldown(ctx context.Context, c domain.Channel, d time.Duration) error {
	d = min(d, MaxCooldown)
	if d <= 0 {
		return nil
	}
	key := l.cooldownKey(c)
	_, err := l.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.SetNX(ctx, key, 1, d)
		// Extend, never shorten, an existing cool-down. go-redis has no millisecond GT variant.
		p.Do(ctx, "PEXPIRE", key, d.Milliseconds(), "GT")
		return nil
	})
	if err != nil {
		return fmt.Errorf("set rate-limit cool-down: %w", err)
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

// IsUnavailable reports whether err came from Redis rather than from ctx ending.
func IsUnavailable(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// Package ratelimit enforces the per-channel send rate across every worker replica (FR-2.2,
// ADR-0006). It is a reserving GCRA in Redis: one atomic Lua script per token reserves the next
// free send slot of the channel (slots are 1/rate apart) and the caller sleeps until its slot. A
// shared cool-down key lets one worker that is throttled by the provider (429) pause the whole
// fleet for that channel.
//
// Reserving instead of "try, and retry after RetryAfter" matters with burst 1: a rejected try
// leaves its slot to whoever asks first, and the latency of the retry is lost, so contending
// replicas fall well below the rate. A reservation is never lost: the slot schedule advances by
// exactly 1/rate per token, whatever the number of workers.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// Limit is the rate of one channel.
type Limit struct {
	// PerSecond is the sustained rate. It should leave headroom below the provider's own limit.
	PerSecond int
	// Burst is how many sends may go out back to back after an idle period. 1 spaces every send
	// 1/PerSecond apart, so no one-second window exceeds PerSecond.
	Burst int
}

// MaxCooldown caps a provider's Retry-After, so a bogus value cannot stall a channel for long.
const MaxCooldown = time.Minute

// maxReserveAhead bounds how far into the future a slot is reserved. Beyond it the caller waits
// without reserving and asks again, so a worker that is cancelled while waiting holds no distant
// slot that would idle the channel.
const maxReserveAhead = time.Second

// reserve atomically reserves the next send slot of KEYS[1] unless the channel is cooling down.
//
//	KEYS[1]  slot schedule: the theoretical arrival time (TAT) of the next slot, in µs
//	KEYS[2]  cool-down flag (PX expiry)
//	ARGV[1]  emission interval in µs (1s / rate)
//	ARGV[2]  burst tolerance in µs ((burst-1) × interval)
//	ARGV[3]  maximum reservation ahead in µs
//
// Returns {state, µs}: {0, wait} reserved, send after wait; {1, wait} not reserved, the next slot
// is too far ahead, ask again after wait; {2, ttl} cooling down for ttl.
var reserve = redis.NewScript(`
local cooldown = redis.call('PTTL', KEYS[2])
if cooldown > 0 then
  return {2, cooldown * 1000}
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local tat = tonumber(redis.call('GET', KEYS[1]) or now)
if tat < now then
  tat = now
end
local wait = tat - tonumber(ARGV[2]) - now
if wait < 0 then
  wait = 0
end
if wait > tonumber(ARGV[3]) then
  return {1, wait - tonumber(ARGV[3])}
end
local next_tat = tat + interval
redis.call('SET', KEYS[1], string.format('%d', next_tat), 'PX', math.ceil((next_tat - now) / 1000) + 1000)
return {0, wait}
`)

type limit struct {
	interval  time.Duration
	tolerance time.Duration
}

// Limiter hands out send slots. It is safe for concurrent use.
type Limiter struct {
	rdb      redis.UniversalClient
	provider string
	limits   map[domain.Channel]limit
	sleep    func(context.Context, time.Duration) error
}

// New returns a limiter for the provider endpoint named provider. Every channel that will ask for
// slots must have a limit.
func New(rdb redis.UniversalClient, provider string, limits map[domain.Channel]Limit) (*Limiter, error) {
	l := &Limiter{rdb: rdb, provider: provider, limits: map[domain.Channel]limit{}, sleep: sleep}
	for c, lim := range limits {
		if lim.PerSecond <= 0 || lim.Burst <= 0 {
			return nil, fmt.Errorf("rate limit of %s must be positive, got %d/s burst %d", c, lim.PerSecond, lim.Burst)
		}
		interval := time.Second / time.Duration(lim.PerSecond)
		l.limits[c] = limit{interval: interval, tolerance: time.Duration(lim.Burst-1) * interval}
	}
	return l, nil
}

// Key is the slot-schedule key of a channel. The braces are a Redis Cluster hash tag, so the
// schedule and its cool-down live in the same slot and one script can use both.
func (l *Limiter) Key(c domain.Channel) string {
	return "{ratelimit:" + string(c) + ":" + l.provider + "}"
}

func (l *Limiter) cooldownKey(c domain.Channel) string { return l.Key(c) + ":cooldown" }

// Wait blocks until channel c may send once, or ctx ends. It honours a fleet-wide cool-down. A
// Redis error is returned at once: the caller must not send without a slot (fail closed,
// ADR-0006).
func (l *Limiter) Wait(ctx context.Context, c domain.Channel) error {
	lim, ok := l.limits[c]
	if !ok {
		return fmt.Errorf("no rate limit configured for channel %s", c)
	}
	keys := []string{l.Key(c), l.cooldownKey(c)}
	for {
		res, err := reserve.Run(ctx, l.rdb, keys,
			lim.interval.Microseconds(), lim.tolerance.Microseconds(), maxReserveAhead.Microseconds()).Int64Slice()
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return fmt.Errorf("reserve a send slot: %w", err)
		}
		if len(res) != 2 {
			return fmt.Errorf("reserve a send slot: unexpected reply %v", res)
		}
		wait := time.Duration(res[1]) * time.Microsecond
		if err := l.sleep(ctx, wait); err != nil {
			return err
		}
		if res[0] == 0 {
			return nil // the reserved slot has arrived
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
	if d <= 0 {
		return ctx.Err()
	}
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

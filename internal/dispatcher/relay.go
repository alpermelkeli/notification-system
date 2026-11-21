package dispatcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/alpermelkeli/notification-system/internal/storage"
)

// RelayStore is the persistence the relay needs. *storage.Store implements it.
type RelayStore interface {
	DispatchDue(ctx context.Context, limit int, publish storage.PublishFunc) (int, error)
	ListenDue(ctx context.Context, notify func()) error
}

// RelayOptions configures a Relay.
type RelayOptions struct {
	// BatchSize is the maximum number of rows locked and published per transaction. Default 500.
	BatchSize int
	// PollInterval is the fallback poll period. It bounds the delay of scheduled and retrying
	// rows, which no NOTIFY announces. Default 500ms.
	PollInterval time.Duration
}

// Relay publishes due notifications in next_attempt_at order (ADR-0003). It wakes on
// NOTIFY notifications_due, which the API sends for rows due now, and polls as a fallback.
type Relay struct {
	store   RelayStore
	publish storage.PublishFunc
	logger  *slog.Logger
	opts    RelayOptions
	wake    chan struct{}
}

// NewRelay returns a relay that publishes with publish.
func NewRelay(store RelayStore, publish storage.PublishFunc, logger *slog.Logger, opts RelayOptions) *Relay {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	return &Relay{store: store, publish: publish, logger: logger, opts: opts, wake: make(chan struct{}, 1)}
}

// Run relays until ctx ends.
func (r *Relay) Run(ctx context.Context) {
	go r.listen(ctx)

	backoff := newBackoff(100*time.Millisecond, 5*time.Second)
	for {
		n, err := r.store.DispatchDue(ctx, r.opts.BatchSize, r.publish)
		if ctx.Err() != nil {
			return
		}
		var wait time.Duration
		switch {
		case err != nil:
			// Kafka or PostgreSQL is unavailable. The rows stay due and nothing is lost.
			wait = backoff.next()
			r.logger.Error("dispatch failed", "err", err, "retry_in", wait)
		case n == r.opts.BatchSize:
			backoff.reset()
			continue // more rows are probably due: drain before waiting
		default:
			backoff.reset()
			if n > 0 {
				r.logger.Debug("dispatched", "count", n)
			}
			wait = r.opts.PollInterval
		}
		if !r.sleep(ctx, wait, err == nil) {
			return
		}
	}
}

// sleep waits for d or, if wakeable, a NOTIFY. It returns false when ctx ends.
func (r *Relay) sleep(ctx context.Context, d time.Duration, wakeable bool) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	wake := r.wake
	if !wakeable {
		wake = nil // after an error, back off even if notifications arrive
	}
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
	case <-wake:
	}
	return true
}

// listen forwards NOTIFY notifications_due to the relay loop, reconnecting after failures.
func (r *Relay) listen(ctx context.Context) {
	backoff := newBackoff(time.Second, 30*time.Second)
	for {
		start := time.Now()
		err := r.store.ListenDue(ctx, func() {
			select {
			case r.wake <- struct{}{}:
			default: // a wake-up is already pending
			}
		})
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			backoff.reset()
		}
		wait := backoff.next()
		r.logger.Warn("listen for due notifications failed; polling only", "err", err, "retry_in", wait)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// backoff doubles a delay from min up to max.
type backoff struct {
	min, max, cur time.Duration
}

func newBackoff(minD, maxD time.Duration) *backoff { return &backoff{min: minD, max: maxD} }

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.min
	} else {
		b.cur = min(2*b.cur, b.max)
	}
	return b.cur
}

func (b *backoff) reset() { b.cur = 0 }

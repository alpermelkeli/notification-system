package worker

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/provider"
)

// Claim retries while the row is still in a dispatchable status: the dispatcher commits queued
// only after Kafka acknowledged the record, so a fast worker can see the record first
// (ADR-0003 amendment). Records that still find the row unclaimable are skipped, and the reaper
// re-publishes the row if it was in fact lost.
var claimRetries = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}

// persistRetries bounds retries of an outcome write that failed because PostgreSQL was
// unavailable. If all fail, the lease expires and the reaper retries the notification.
var persistRetries = []time.Duration{200 * time.Millisecond, time.Second, 2 * time.Second}

// process delivers one record: guarded claim, provider call, outcome.
func (w *Worker) process(ctx context.Context, r record) {
	ctx = withCorrelation(ctx, r)
	log := w.logger.With("notification_id", r.msg.ID.String(), "priority", string(r.msg.Priority))

	n, err := w.claim(ctx, r)
	if errors.Is(err, errStale) {
		log.DebugContext(ctx, "skipping stale work record", "err", err)
		return
	}
	if err != nil {
		// The row stays queued; if this record is committed past, the reaper re-publishes it.
		log.ErrorContext(ctx, "claim failed", "err", err)
		return
	}
	lease, _ := n.Lease()
	log = log.With("attempt", n.Attempts)

	res, sendErr := w.sender.Send(ctx, provider.Message{
		IdempotencyKey: n.ID.String(),
		CorrelationID:  n.CorrelationID,
		To:             n.Recipient,
		Channel:        n.Channel,
		Content:        n.Content,
	})

	var outcome func() error
	switch {
	case sendErr == nil:
		outcome = func() error { _, err := w.store.MarkSent(ctx, lease, res.MessageID); return err }
		log.DebugContext(ctx, "delivered", "provider_message_id", res.MessageID)
	case n.Attempts >= w.opts.MaxAttempts:
		outcome = func() error { _, err := w.store.MarkFailed(ctx, lease, sendErr.Error()); return err }
		log.ErrorContext(ctx, "delivery failed; attempts exhausted", "err", sendErr)
	default:
		delay := w.retryDelay(ctx, sendErr)
		outcome = func() error { _, err := w.store.MarkRetrying(ctx, lease, delay, sendErr.Error()); return err }
		log.WarnContext(ctx, "delivery failed; will retry", "err", sendErr, "retry_in", delay)
	}
	if err := w.persist(ctx, outcome); err != nil {
		if errors.Is(err, domain.ErrLeaseLost) {
			// The lease expired and the row was reaped; the newer claim owns the outcome.
			log.WarnContext(ctx, "outcome discarded: lease lost", "err", err)
			return
		}
		log.ErrorContext(ctx, "outcome not persisted; the lease will expire and be retried", "err", err)
	}
}

// retryDelay returns the delay before the next attempt. A 429 also pauses the whole fleet for the
// provider's Retry-After (ADR-0006). Phase 6 replaces the fixed delay with exponential backoff and
// classifies permanent failures (ADR-0005).
func (w *Worker) retryDelay(ctx context.Context, sendErr error) time.Duration {
	delay := w.opts.RetryDelay
	var pe *provider.Error
	if errors.As(sendErr, &pe) && pe.StatusCode == http.StatusTooManyRequests {
		cool := pe.RetryAfter
		if cool <= 0 {
			cool = time.Second
		}
		if err := w.limiter.Cooldown(ctx, w.channel, cool); err != nil {
			w.logger.WarnContext(ctx, "could not set the rate-limit cool-down", "err", err)
		}
		delay = max(delay, cool)
	}
	return delay
}

// claim claims the record's notification, retrying briefly while the dispatcher's commit may
// still be in flight. It returns errStale when the record must be skipped.
func (w *Worker) claim(ctx context.Context, r record) (domain.Notification, error) {
	for i := 0; ; i++ {
		n, err := w.store.Claim(ctx, r.msg.ID, r.msg.CreatedAt, w.opts.Lease)
		if err == nil {
			return n, nil
		}
		var te *domain.TransitionError
		switch {
		case errors.As(err, &te) && dispatchable(te.Current) && i < len(claimRetries):
			sleep(ctx, claimRetries[i])
			continue
		case errors.As(err, &te), errors.Is(err, domain.ErrNotFound):
			// Cancelled, already claimed or finished, or a duplicate publish (ADR-0004).
			return domain.Notification{}, errors.Join(errStale, err)
		}
		return domain.Notification{}, err
	}
}

func dispatchable(s domain.Status) bool {
	for _, d := range domain.DispatchableStatuses {
		if s == d {
			return true
		}
	}
	return false
}

// persist runs an outcome write, retrying while PostgreSQL is unavailable. Guard failures
// (lease lost, invalid transition) are final.
func (w *Worker) persist(ctx context.Context, write func() error) error {
	err := write()
	for _, d := range persistRetries {
		if err == nil || errors.Is(err, domain.ErrLeaseLost) || errors.Is(err, domain.ErrInvalidTransition) ||
			ctx.Err() != nil {
			return err
		}
		sleep(ctx, d)
		err = write()
	}
	return err
}

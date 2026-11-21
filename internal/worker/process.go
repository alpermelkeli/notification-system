package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/ratelimit"
)

// staleToken is how old a token may be when the claim finishes; older tokens are replaced.
const staleToken = 50 * time.Millisecond

// persistRetries bounds retries of an outcome write that failed because PostgreSQL was
// unavailable. If all fail, the lease expires and the reaper retries the notification.
var persistRetries = []time.Duration{200 * time.Millisecond, time.Second, 2 * time.Second}

// process delivers one record: guarded claim, provider call, outcome. done reports the send to
// the circuit breaker that admitted it; it is called exactly once.
func (w *Worker) process(ctx context.Context, r record, done func(error)) {
	ctx = withCorrelation(ctx, r)
	log := w.logger.With("notification_id", r.msg.ID.String(), "priority", string(r.msg.Priority))
	reported := false
	report := func(err error) {
		if !reported {
			reported = true
			done(err)
		}
	}
	defer report(errNotAttempted)

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

	// The breaker admitted this send before the claim. If it opened since (sends in flight
	// failed), do not add to the provider's trouble: hand the attempt back (ADR-0005).
	if w.circuit.open() {
		log.InfoContext(ctx, "circuit breaker opened after the claim; releasing")
		w.release(ctx, log, lease, w.circuit.timeout, "circuit breaker open")
		return
	}

	// The token was granted before the claim. If the claim was slow (e.g. it waited for the
	// dispatcher's commit), sends whose claims finished together would reach the provider in a
	// burst above the rate, so take a fresh token (ADR-0006).
	if time.Since(r.tokenAt) > staleToken {
		if err := w.limiter.Wait(ctx, w.channel); err != nil {
			// No token, no send. Hand the attempt back; it is retried without consuming it.
			log.WarnContext(ctx, "no rate-limit token after claim; releasing", "err", err)
			w.release(ctx, log, lease, w.opts.Retry.BaseDelay, "rate limiter unavailable")
			return
		}
	}

	res, sendErr := w.sender.Send(ctx, provider.Message{
		IdempotencyKey: n.ID.String(),
		CorrelationID:  n.CorrelationID,
		To:             n.Recipient,
		Channel:        n.Channel,
		Content:        n.Content,
	})
	report(sendErr)
	w.settle(ctx, log, n, lease, res, sendErr)
}

// release hands a claimed notification back to retrying after delay without consuming the
// attempt.
func (w *Worker) release(ctx context.Context, log *slog.Logger, lease domain.Lease, delay time.Duration, reason string) {
	if err := w.persist(ctx, func() error {
		_, err := w.store.Release(ctx, lease, delay, reason)
		return err
	}); err != nil {
		log.ErrorContext(ctx, "release not persisted; the lease will expire and be retried", "err", err)
	}
}

// settle persists the outcome of a send (ADR-0005, FR-3.1–3.3):
//
//	2xx                      → sent
//	permanent (other 4xx)    → failed + dead letter
//	retryable or 429, budget → retrying after the jittered backoff (429: at least Retry-After)
//	retryable or 429, last   → failed + dead letter
func (w *Worker) settle(ctx context.Context, log *slog.Logger, n domain.Notification, lease domain.Lease,
	res provider.Result, sendErr error,
) {
	class := provider.Classify(sendErr)
	var cool time.Duration
	if class == provider.ClassRateLimited {
		cool = w.cooldown(ctx, sendErr) // pauses the fleet even when this attempt was the last
	}

	var (
		outcome func() error
		failed  *domain.Notification
	)
	fail := func(cause string) {
		outcome = func() error {
			row, err := w.store.MarkFailed(ctx, lease, cause)
			if err == nil {
				failed = &row
			}
			return err
		}
	}
	switch {
	case class == provider.ClassDelivered:
		outcome = func() error { _, err := w.store.MarkSent(ctx, lease, res.MessageID); return err }
		log.DebugContext(ctx, "delivered", "provider_message_id", res.MessageID)
	case class == provider.ClassPermanent:
		fail(class.String() + ": " + sendErr.Error())
		log.ErrorContext(ctx, "delivery failed permanently", "err", sendErr)
	case w.opts.Retry.Exhausted(n.Attempts):
		fail(fmt.Sprintf("attempts exhausted (%d), last %s: %s", n.Attempts, class, sendErr))
		log.ErrorContext(ctx, "delivery failed; attempts exhausted", "err", sendErr, "class", class.String())
	default:
		delay := max(w.opts.Retry.Delay(n.Attempts), cool)
		cause := class.String() + ": " + sendErr.Error()
		outcome = func() error { _, err := w.store.MarkRetrying(ctx, lease, delay, cause); return err }
		log.WarnContext(ctx, "delivery failed; will retry", "err", sendErr, "class", class.String(), "retry_in", delay)
	}

	if err := w.persist(ctx, outcome); err != nil {
		if errors.Is(err, domain.ErrLeaseLost) {
			// The lease expired and the row was reaped; the newer claim owns the outcome.
			log.WarnContext(ctx, "outcome discarded: lease lost", "err", err)
			return
		}
		log.ErrorContext(ctx, "outcome not persisted; the lease will expire and be retried", "err", err)
		return
	}
	if failed != nil && w.opts.DeadLetters != nil {
		// Best effort, after the commit: the failed row is the source of truth (ADR-0005).
		if err := w.opts.DeadLetters.Publish(ctx, *failed); err != nil {
			log.WarnContext(ctx, "dead letter not published", "err", err)
		}
	}
}

// cooldown handles a 429: it pauses the whole fleet's sends on this channel for the provider's
// Retry-After (ADR-0006) and returns the pause, which is also the minimum delay of the retry.
func (w *Worker) cooldown(ctx context.Context, sendErr error) time.Duration {
	var pe *provider.Error
	cool := time.Second
	if errors.As(sendErr, &pe) && pe.RetryAfter > 0 {
		cool = min(pe.RetryAfter, ratelimit.MaxCooldown) // a bogus Retry-After must not park the row for days
	}
	if err := w.limiter.Cooldown(ctx, w.channel, cool); err != nil {
		w.logger.WarnContext(ctx, "could not set the rate-limit cool-down", "err", err)
	}
	return cool
}

// claim claims the record's notification. If the dispatcher has not committed the row as queued
// yet, the claim waits on its row lock (storage.apply), so an early record is not skipped. It
// returns errStale when the record must be skipped.
func (w *Worker) claim(ctx context.Context, r record) (domain.Notification, error) {
	n, err := w.store.Claim(ctx, r.msg.ID, r.msg.CreatedAt, w.opts.Lease)
	var te *domain.TransitionError
	if errors.As(err, &te) || errors.Is(err, domain.ErrNotFound) {
		// Cancelled, already claimed or finished, or a duplicate publish (ADR-0004).
		return domain.Notification{}, errors.Join(errStale, err)
	}
	return n, err
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

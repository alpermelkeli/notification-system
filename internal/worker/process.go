package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/ratelimit"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// staleToken is how old a token may be when the claim finishes; older tokens are replaced.
const staleToken = 50 * time.Millisecond

// Claim retry while PostgreSQL is unavailable: each try is bounded by claimTimeout, and the waits
// double from claimBackoffMin to claimBackoffMax.
const (
	claimTimeout    = 5 * time.Second
	claimBackoffMin = 100 * time.Millisecond
	claimBackoffMax = 5 * time.Second
)

// persistRetries bounds retries of an outcome write that failed because PostgreSQL was
// unavailable. If all fail, the lease expires and the reaper retries the notification.
var persistRetries = []time.Duration{200 * time.Millisecond, time.Second, 2 * time.Second}

// process delivers one record: guarded claim, provider call, outcome. done reports the send to
// the circuit breaker that admitted it; it is called exactly once. It returns false if the record
// was given up without a decision (shutdown or revoke during a database outage) and must stay
// below the watermark.
//
// runCtx is the worker's run context. Once claimed, a send is not abandoned on shutdown: it finishes
// and persists its outcome within the lease, so the record is not delivered twice.
func (w *Worker) process(runCtx context.Context, r record, done func(error)) bool {
	runCtx = withCorrelation(runCtx, r)
	log := w.logger.With("notification_id", r.msg.ID.String(), "priority", string(r.msg.Priority))
	reported := false
	report := func(err error) {
		if !reported {
			reported = true
			done(err)
		}
	}
	defer report(errNotAttempted)

	n, err := w.claimWithRetry(runCtx, log, r)
	switch {
	case errors.Is(err, errStale):
		log.DebugContext(runCtx, "skipping stale work record", "err", err)
		return true
	case errors.Is(err, errAbandoned):
		log.InfoContext(runCtx, "work record left for the next owner", "err", err)
		return false
	case err != nil:
		// Not an outage but a failing statement, which retrying cannot fix. The row stays queued;
		// once this record is committed past, the reaper re-publishes it.
		log.ErrorContext(runCtx, "claim failed", "err", err)
		return true
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(runCtx), w.opts.Lease)
	defer cancel()
	lease, _ := n.Lease()
	log = log.With("attempt", n.Attempts)

	// The breaker admitted this send before the claim. If it opened since (sends in flight
	// failed), do not add to the provider's trouble: hand the attempt back (ADR-0005).
	if w.circuit.open() {
		log.InfoContext(ctx, "circuit breaker opened after the claim; releasing")
		w.release(ctx, log, lease, w.circuit.timeout, "circuit breaker open")
		return true
	}

	// The token was granted before the claim. If the claim was slow (e.g. it waited for the
	// dispatcher's commit), sends whose claims finished together would reach the provider in a
	// burst above the rate, so take a fresh token (ADR-0006).
	if time.Since(r.tokenAt) > staleToken {
		if err := w.limiter.Wait(ctx, w.channel); err != nil {
			// No token, no send. Hand the attempt back; it is retried without consuming it.
			log.WarnContext(ctx, "no rate-limit token after claim; releasing", "err", err)
			w.release(ctx, log, lease, w.opts.Retry.BaseDelay, "rate limiter unavailable")
			return true
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
	return true
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

// errAbandoned means the claim was given up during a database outage because the worker is
// shutting down or the partition is being revoked.
var errAbandoned = errors.New("claim abandoned")

// claimWithRetry claims the record's notification. While PostgreSQL is unavailable it keeps
// retrying with backoff instead of giving up: a record given up would be committed past, so the
// workers would drain the Kafka backlog during an outage and recovery would wait for the reaper,
// which re-publishes by age and not by priority. Holding the claim also holds this sender slot,
// so the worker stops taking work until the database is back. It gives up with errAbandoned only
// on shutdown or revoke; the record then stays below the watermark.
func (w *Worker) claimWithRetry(ctx context.Context, log *slog.Logger, r record) (domain.Notification, error) {
	backoff := claimBackoffMin
	token := uuid.New() // the same token on every try makes a retried claim idempotent
	for try := 1; ; try++ {
		// Each try runs to completion even on shutdown: abandoning a claim that committed would
		// leave the row processing until its lease expires.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTimeout)
		n, err := w.claim(cctx, r, token)
		cancel()
		if err == nil || !storage.IsTransient(err) {
			if err == nil && try > 1 {
				log.InfoContext(ctx, "claim succeeded after database errors", "tries", try)
			}
			return n, err
		}
		if try == 1 || try%10 == 0 {
			log.WarnContext(ctx, "database unavailable; retrying the claim", "err", err, "tries", try)
		}
		// Wait in short steps, so a revoke does not wait out a long backoff.
		for waited := time.Duration(0); waited < backoff; waited += claimBackoffMin {
			if ctx.Err() != nil || w.tracker.gone(r.part) {
				return domain.Notification{}, errors.Join(errAbandoned, err)
			}
			sleep(ctx, claimBackoffMin)
		}
		backoff = min(backoff*2, claimBackoffMax)
	}
}

// claim claims the record's notification. If the dispatcher has not committed the row as queued
// yet, the claim waits on its row lock (storage.apply), so an early record is not skipped. It
// returns errStale when the record must be skipped.
func (w *Worker) claim(ctx context.Context, r record, token uuid.UUID) (domain.Notification, error) {
	n, err := w.store.Claim(ctx, r.msg.ID, r.msg.CreatedAt, w.opts.Lease, token)
	var te *domain.TransitionError
	if errors.As(err, &te) || errors.Is(err, domain.ErrNotFound) {
		// Cancelled, already claimed or finished, or a duplicate publish (ADR-0004).
		return domain.Notification{}, errors.Join(errStale, err)
	}
	return n, err
}

// persist runs an outcome write, retrying while PostgreSQL is unavailable. Other errors, such as
// guard failures (lease lost, invalid transition), are final.
func (w *Worker) persist(ctx context.Context, write func() error) error {
	err := write()
	for _, d := range persistRetries {
		if err == nil || !storage.IsTransient(err) || ctx.Err() != nil {
			return err
		}
		sleep(ctx, d)
		err = write()
	}
	return err
}

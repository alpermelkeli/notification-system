package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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

// Time budget of a claimed send, measured from the start of the claim, which is no later than
// the database's lease start:
//   - the send must finish outcomeReserve before the lease ends, so the outcome can be written
//     while the lease still fences it (config requires LEASE_DURATION >= PROVIDER_TIMEOUT + 15s,
//     which covers claimTimeout, tokenWaitMax and outcomeReserve);
//   - a fresh token after a slow claim is waited for at most tokenWaitMax: a long fleet
//     cool-down must not eat the lease, so the attempt is handed back instead;
//   - outcome writes are retried while PostgreSQL is unavailable until the lease ends, each try
//     bounded by writeTimeout. After that the reaper owns the row.
const (
	outcomeReserve  = 5 * time.Second
	tokenWaitMax    = 2 * time.Second
	writeTimeout    = 2 * time.Second
	writeBackoffMin = 200 * time.Millisecond
	writeBackoffMax = 2 * time.Second
)

// process delivers one record: guarded claim, provider call, outcome. adm is the circuit
// breaker's admission of the send; its done is called exactly once. It returns false if the
// record was given up without a decision (shutdown or revoke during a database outage) and must
// stay below the watermark.
//
// runCtx is the worker's run context. Once claimed, a send is not abandoned on shutdown: it
// finishes and persists its outcome within the lease, so the record is not delivered twice.
func (w *Worker) process(runCtx context.Context, r record, adm admission) bool {
	runCtx = withCorrelation(runCtx, r)
	log := w.logger.With("notification_id", r.msg.ID.String(), "priority", string(r.msg.Priority))
	reported := false
	report := func(err error) {
		if !reported {
			reported = true
			adm.done(err)
		}
	}
	defer report(errNotAttempted)

	n, claimStart, err := w.claimWithRetry(runCtx, log, r)
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
	c := claimed{n: n, leaseEnd: claimStart.Add(w.opts.Lease)}
	c.lease, _ = n.Lease()
	base := context.WithoutCancel(runCtx)
	log = log.With("attempt", n.Attempts)

	// The breaker admitted this send before the claim. If it stopped admitting since (sends in
	// flight failed), do not add to the provider's trouble: hand the attempt back (ADR-0005).
	if w.circuit.revoked(adm) {
		log.InfoContext(runCtx, "circuit breaker opened after the claim; releasing")
		w.release(base, log, c, w.circuit.timeout, "circuit breaker open")
		return true
	}

	sendDeadline := c.leaseEnd.Add(-outcomeReserve)
	// The token was granted before the claim. If the claim was slow (e.g. it waited for the
	// dispatcher's commit), sends whose claims finished together would reach the provider in a
	// burst above the rate, so take a fresh token (ADR-0006).
	if time.Since(r.tokenAt) > staleToken {
		wctx, cancel := context.WithTimeout(base, min(tokenWaitMax, time.Until(sendDeadline)))
		err := w.limiter.Wait(wctx, w.channel)
		cancel()
		if err != nil {
			// No token, no send. Hand the attempt back; it is retried without consuming it.
			log.WarnContext(runCtx, "no rate-limit token after claim; releasing", "err", err)
			w.release(base, log, c, w.opts.Retry.BaseDelay, "no rate-limit token")
			return true
		}
	}

	sctx, cancel := context.WithDeadline(base, sendDeadline)
	res, sendErr := w.sender.Send(sctx, provider.Message{
		IdempotencyKey: n.ID.String(),
		CorrelationID:  n.CorrelationID,
		To:             n.Recipient,
		Channel:        n.Channel,
		Content:        n.Content,
	})
	cancel()
	report(sendErr)
	w.settle(base, log, c, res, sendErr)
	return true
}

// claimed is a notification this worker holds a lease on.
type claimed struct {
	n     domain.Notification
	lease domain.Lease
	// leaseEnd is a local bound of the lease: the claim started before the database's now(), so
	// the real lease ends no earlier.
	leaseEnd time.Time
}

// release hands a claimed notification back to retrying after delay without consuming the
// attempt.
func (w *Worker) release(ctx context.Context, log *slog.Logger, c claimed, delay time.Duration, reason string) {
	_, err := w.persist(ctx, c, func(ctx context.Context) error {
		_, err := w.store.Release(ctx, c.lease, delay, reason)
		return err
	})
	if err != nil {
		log.ErrorContext(ctx, "release not persisted; the lease will expire and be retried", "err", err)
	}
}

// settle persists the outcome of a send (ADR-0005, FR-3.1–3.3):
//
//	2xx                    → sent
//	permanent (other 4xx)  → failed + dead letter
//	429                    → released: retried after max(backoff, Retry-After) without consuming
//	                         the attempt, since the provider is healthy and only asks us to slow down
//	retryable, budget left → retrying after the jittered backoff
//	retryable, last        → failed + dead letter
func (w *Worker) settle(ctx context.Context, log *slog.Logger, c claimed, res provider.Result, sendErr error) {
	n := c.n
	class := provider.Classify(sendErr)
	var (
		target domain.Status
		cause  string // last_error written with the outcome, empty for sent
		write  func(context.Context) (domain.Notification, error)
	)
	fail := func(why string) {
		target, cause = domain.StatusFailed, why
		write = func(ctx context.Context) (domain.Notification, error) { return w.store.MarkFailed(ctx, c.lease, why) }
	}
	switch {
	case class == provider.ClassDelivered:
		target = domain.StatusSent
		write = func(ctx context.Context) (domain.Notification, error) {
			return w.store.MarkSent(ctx, c.lease, res.MessageID)
		}
		log.DebugContext(ctx, "delivered", "provider_message_id", res.MessageID)
	case class == provider.ClassPermanent:
		fail(class.String() + ": " + sendErr.Error())
		log.ErrorContext(ctx, "delivery failed permanently", "err", sendErr)
	case class == provider.ClassRateLimited:
		delay := max(w.opts.Retry.Delay(n.Attempts), w.cooldown(ctx, sendErr))
		log.WarnContext(ctx, "rate limited by the provider; releasing", "err", sendErr, "retry_in", delay)
		w.release(ctx, log, c, delay, class.String()+": "+sendErr.Error())
		return
	case w.opts.Retry.Exhausted(n.Attempts):
		fail(fmt.Sprintf("attempts exhausted (%d), last %s: %s", n.Attempts, class, sendErr))
		log.ErrorContext(ctx, "delivery failed; attempts exhausted", "err", sendErr, "class", class.String())
	default:
		delay := w.opts.Retry.Delay(n.Attempts)
		cause = class.String() + ": " + sendErr.Error()
		target = domain.StatusRetrying
		write = func(ctx context.Context) (domain.Notification, error) {
			return w.store.MarkRetrying(ctx, c.lease, delay, cause)
		}
		log.WarnContext(ctx, "delivery failed; will retry", "err", sendErr, "class", class.String(), "retry_in", delay)
	}

	var row domain.Notification
	ambiguous, err := w.persist(ctx, c, func(ctx context.Context) error {
		var err error
		row, err = write(ctx)
		return err
	})
	if err != nil && ambiguous && isGuardError(err) {
		// An earlier try may have committed before its answer was lost; then the guard rejects
		// the retry. Check whether the row holds this outcome, and not the reaper's.
		gctx, cancel := context.WithTimeout(ctx, writeTimeout)
		got, gerr := w.store.GetNotification(gctx, n.ID)
		cancel()
		if gerr == nil && isOurOutcome(got, n.Attempts, target, cause) {
			row, err = got, nil
		}
	}
	if err != nil {
		if errors.Is(err, domain.ErrLeaseLost) {
			// The lease expired and the row was reaped; the newer claim owns the outcome.
			log.WarnContext(ctx, "outcome discarded: lease lost", "err", err)
			return
		}
		log.ErrorContext(ctx, "outcome not persisted; the lease will expire and be retried", "err", err)
		return
	}
	if target == domain.StatusFailed && w.opts.DeadLetters != nil {
		// Best effort, after the commit: the failed row is the source of truth (ADR-0005).
		dctx, cancel := context.WithTimeout(ctx, writeTimeout+outcomeReserve)
		defer cancel()
		if err := w.opts.DeadLetters.Publish(dctx, row); err != nil {
			log.WarnContext(ctx, "dead letter not published", "err", err)
		}
	}
}

// isOurOutcome reports whether got is the outcome this worker wrote for its attempt. The reaper
// can leave the same status and attempt count (e.g. failed on the last attempt), but with its own
// last_error; a sent row can only come from the lease holder.
func isOurOutcome(got domain.Notification, attempt int, target domain.Status, cause string) bool {
	if got.Status != target || got.Attempts != attempt {
		return false
	}
	if target == domain.StatusSent {
		return true
	}
	// last_error is stored truncated, so compare as a prefix.
	return got.LastError != nil && *got.LastError != "" && strings.HasPrefix(cause, *got.LastError)
}

func isGuardError(err error) bool {
	return errors.Is(err, domain.ErrLeaseLost) || errors.Is(err, domain.ErrInvalidTransition)
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
//
// It also returns when the successful try started, from which the caller bounds the lease.
func (w *Worker) claimWithRetry(ctx context.Context, log *slog.Logger, r record) (domain.Notification, time.Time, error) {
	backoff := claimBackoffMin
	token := uuid.New() // the same token on every try makes a retried claim idempotent
	for try := 1; ; try++ {
		// Each try runs to completion even on shutdown: abandoning a claim that committed would
		// leave the row processing until its lease expires.
		start := time.Now()
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTimeout)
		n, err := w.claim(cctx, r, token)
		cancel()
		if err == nil || !storage.IsTransient(err) {
			if err == nil && try > 1 {
				log.InfoContext(ctx, "claim succeeded after database errors", "tries", try)
			}
			return n, start, err
		}
		switch {
		case try == 1:
			log.WarnContext(ctx, "database unavailable; retrying the claim", "err", err)
		case try%30 == 0: // about every two minutes at the maximum backoff
			log.ErrorContext(ctx, "database still unavailable; the claim keeps waiting", "err", err, "tries", try)
		}
		// Wait in short steps, so a revoke does not wait out a long backoff.
		for waited := time.Duration(0); waited < backoff; waited += claimBackoffMin {
			if ctx.Err() != nil || w.tracker.gone(r.part) {
				return domain.Notification{}, time.Time{}, errors.Join(errAbandoned, err)
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

// persist runs a write fenced by c's lease, retrying while PostgreSQL is unavailable until the
// lease ends; each try has its own deadline, so a write after a long send still gets its full
// budget. Other errors, such as guard failures (lease lost, invalid transition), are final.
// ambiguous reports that an earlier try failed transiently and may have committed nonetheless.
func (w *Worker) persist(ctx context.Context, c claimed, write func(context.Context) error) (ambiguous bool, err error) {
	backoff := writeBackoffMin
	for {
		wctx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = write(wctx)
		cancel()
		if err == nil || !storage.IsTransient(err) || time.Now().Add(backoff).After(c.leaseEnd) {
			return ambiguous, err
		}
		ambiguous = true
		sleep(ctx, backoff)
		backoff = min(backoff*2, writeBackoffMax)
	}
}

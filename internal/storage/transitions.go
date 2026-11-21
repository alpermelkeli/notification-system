package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// inTx runs fn in a transaction, committing if it returns nil.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, fn)
}

// transition describes one guarded status change.
type transition struct {
	from []domain.Status
	to   domain.Status
	// set holds extra assignments appended to the SET clause, e.g. ", sent_at = now()". It must
	// be a constant: values are passed through args, which are bound from $6 on. The row being
	// updated is aliased n.
	set    string
	args   []any
	reason string
	// lease, if set, additionally requires the row's lease_token to match (fencing, ADR-0004).
	lease *uuid.UUID
}

// validate rejects a transition the domain state machine does not allow. It catches programming
// errors in this package, not bad input.
func (t transition) validate() error {
	for _, from := range t.from {
		if !domain.CanTransition(from, t.to) {
			return fmt.Errorf("storage: transition %s -> %s is not in the state machine", from, t.to)
		}
	}
	return nil
}

// apply runs t against one notification and returns the updated row. The row lock taken in prev
// makes concurrent transitions serialise; READ COMMITTED re-checks the status guard after the
// lock is granted, so exactly one of two racing transitions wins.
func apply(ctx context.Context, q querier, id uuid.UUID, createdAt time.Time, t transition) (domain.Notification, error) {
	if err := t.validate(); err != nil {
		return domain.Notification{}, err
	}
	sql := `
WITH prev AS (
    SELECT id, created_at, status FROM notifications
     WHERE id = $1 AND created_at = $2 AND status = ANY($3)
       AND ($6::uuid IS NULL OR lease_token = $6)
       FOR UPDATE
), upd AS (
    UPDATE notifications n
       SET status = $4, updated_at = now()` + t.set + `
      FROM prev
     WHERE n.id = prev.id AND n.created_at = prev.created_at
 RETURNING ` + columns("n") + `, prev.status AS prev_status
), hist AS (
    INSERT INTO notification_status_history
           (notification_id, notification_created_at, from_status, to_status, attempt, reason)
    SELECT id, created_at, prev_status, status, attempts, $5 FROM upd
)
SELECT ` + columns("") + ` FROM upd`

	args := append([]any{id, createdAt, statusStrings(t.from), string(t.to), t.reason, t.lease}, t.args...)
	n, err := scanNotification(q.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, rejected(ctx, q, id, createdAt, t)
	}
	if err != nil {
		return domain.Notification{}, fmt.Errorf("transition to %s: %w", t.to, err)
	}
	return n, nil
}

// rejected explains why a guarded transition matched no row.
func rejected(ctx context.Context, q querier, id uuid.UUID, createdAt time.Time, t transition) error {
	var (
		current string
		token   *uuid.UUID
	)
	err := q.QueryRow(ctx, `SELECT status, lease_token FROM notifications WHERE id = $1 AND created_at = $2`,
		id, createdAt).Scan(&current, &token)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("read status: %w", err)
	}
	if t.lease != nil && (token == nil || *token != *t.lease) {
		return fmt.Errorf("%w (current status %s)", domain.ErrLeaseLost, current)
	}
	return &domain.TransitionError{Current: domain.Status(current), Target: t.to}
}

func statusStrings(ss []domain.Status) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

// truncate bounds free-text error messages stored in last_error and history.reason.
func truncate(s string) string {
	const maxLen = 1000
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// Cancel moves a pending, queued or retrying notification to cancelled (FR-1.4). A queued
// notification already in Kafka is then dropped by the worker's guarded claim (ADR-0004). It
// returns a *domain.TransitionError if the notification is in any other status.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	if !domain.IsV7(id) {
		return domain.Notification{}, domain.ErrNotFound
	}
	return apply(ctx, s.pool, id, domain.IDTime(id), transition{
		from: domain.CancellableStatuses, to: domain.StatusCancelled, reason: "cancelled by client",
	})
}

// Replay resets a failed notification to pending with a fresh attempt budget (B-1, ADR-0005)
// and wakes the dispatcher.
func (s *Store) Replay(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	if !domain.IsV7(id) {
		return domain.Notification{}, domain.ErrNotFound
	}
	var n domain.Notification
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		n, err = apply(ctx, tx, id, domain.IDTime(id), transition{
			from: []domain.Status{domain.StatusFailed}, to: domain.StatusPending,
			set:    `, attempts = 0, next_attempt_at = now(), queued_at = NULL, last_error = NULL`,
			reason: "replayed",
		})
		if err != nil {
			return err
		}
		return notifyDue(ctx, tx)
	})
	return n, err
}

// Claim moves a queued notification to processing for a worker, increments attempts and issues
// a lease with a fresh fencing token (ADR-0004). The returned row carries the content to deliver
// and its Lease(), which every outcome call must present. A *domain.TransitionError or
// domain.ErrNotFound means the Kafka record is stale (cancelled, already claimed or finished)
// and must be skipped.
func (s *Store) Claim(ctx context.Context, id uuid.UUID, createdAt time.Time, lease time.Duration) (domain.Notification, error) {
	return apply(ctx, s.pool, id, createdAt, transition{
		from: []domain.Status{domain.StatusQueued}, to: domain.StatusProcessing,
		set: `, attempts = n.attempts + 1, lease_token = gen_random_uuid(),
		       lease_until = now() + make_interval(secs => $7)`,
		args:   []any{lease.Seconds()},
		reason: "claimed",
	})
}

// endLease clears the lease when a notification leaves processing.
const endLease = `, lease_until = NULL, lease_token = NULL`

// outcome builds a transition out of processing that is fenced by the lease token.
func outcome(l domain.Lease, to domain.Status, set, reason string, args ...any) transition {
	return transition{
		from: []domain.Status{domain.StatusProcessing}, to: to,
		set: set + endLease, args: args, reason: reason, lease: &l.Token,
	}
}

// MarkSent records a successful delivery. It returns domain.ErrLeaseLost if the lease expired
// and the notification was reaped (and possibly claimed again) in the meantime.
func (s *Store) MarkSent(ctx context.Context, l domain.Lease, providerMessageID string) (domain.Notification, error) {
	return apply(ctx, s.pool, l.NotificationID, l.CreatedAt, outcome(l, domain.StatusSent,
		`, sent_at = now(), provider_message_id = NULLIF($7, ''), last_error = NULL`,
		"delivered", providerMessageID))
}

// MarkRetrying records a retryable failure and schedules the next attempt after delay
// (ADR-0005). The delay is added to the database clock, which the dispatcher compares against.
func (s *Store) MarkRetrying(ctx context.Context, l domain.Lease, delay time.Duration, cause string) (domain.Notification, error) {
	cause = truncate(cause)
	return apply(ctx, s.pool, l.NotificationID, l.CreatedAt, outcome(l, domain.StatusRetrying,
		`, next_attempt_at = now() + make_interval(secs => $7), last_error = $8`,
		cause, delay.Seconds(), cause))
}

// Release returns a claimed notification to retrying after delay without consuming an attempt,
// e.g. when the circuit breaker is open (ADR-0005).
func (s *Store) Release(ctx context.Context, l domain.Lease, delay time.Duration, reason string) (domain.Notification, error) {
	return apply(ctx, s.pool, l.NotificationID, l.CreatedAt, outcome(l, domain.StatusRetrying,
		`, attempts = n.attempts - 1, next_attempt_at = now() + make_interval(secs => $7)`,
		truncate(reason), delay.Seconds()))
}

// MarkFailed records a permanent failure or an exhausted retry budget (FR-3.3).
func (s *Store) MarkFailed(ctx context.Context, l domain.Lease, cause string) (domain.Notification, error) {
	cause = truncate(cause)
	return apply(ctx, s.pool, l.NotificationID, l.CreatedAt, outcome(l, domain.StatusFailed,
		`, last_error = $7`, cause, cause))
}

// DispatchDue implements one dispatcher iteration (ADR-0003). It locks up to limit due
// notifications in next_attempt_at order (FIFO, skipping rows locked by other dispatchers),
// passes them to publish while holding the locks, and marks them queued only if publish
// succeeds. If publish fails, the transaction rolls back and the rows stay due.
//
// A crash between a successful publish and the commit re-publishes the rows later; workers
// discard the duplicates through the guarded claim (ADR-0004).
func (s *Store) DispatchDue(ctx context.Context, limit int, publish func(context.Context, []domain.Notification) error) (int, error) {
	t := transition{from: domain.DispatchableStatuses, to: domain.StatusQueued}
	if err := t.validate(); err != nil {
		return 0, err
	}
	var n int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+columns("")+` FROM notifications
			 WHERE status = ANY($1) AND next_attempt_at <= now()
			 ORDER BY next_attempt_at
			 LIMIT $2
			   FOR UPDATE SKIP LOCKED`, statusStrings(t.from), limit)
		if err != nil {
			return fmt.Errorf("select due: %w", err)
		}
		due, err := collectNotifications(rows)
		if err != nil {
			return fmt.Errorf("scan due: %w", err)
		}
		if len(due) == 0 {
			return nil
		}
		if err := publish(ctx, due); err != nil {
			return fmt.Errorf("publish: %w", err)
		}

		ids := make([]uuid.UUID, len(due))
		createdAts := make([]time.Time, len(due))
		prev := make([]string, len(due))
		for i, d := range due {
			ids[i], createdAts[i], prev[i] = d.ID, d.CreatedAt, string(d.Status)
		}
		tag, err := tx.Exec(ctx, `
WITH b AS (
    SELECT * FROM unnest($1::uuid[], $2::timestamptz[], $3::text[]) AS b(id, created_at, prev_status)
), upd AS (
    UPDATE notifications n
       SET status = 'queued', queued_at = now(), updated_at = now()
      FROM b
     WHERE n.id = b.id AND n.created_at = b.created_at AND n.status = b.prev_status
 RETURNING n.id, n.created_at, n.attempts, b.prev_status
)
INSERT INTO notification_status_history
       (notification_id, notification_created_at, from_status, to_status, attempt, reason)
SELECT id, created_at, prev_status, 'queued', attempts, 'dispatched' FROM upd`, ids, createdAts, prev)
		if err != nil {
			return fmt.Errorf("mark queued: %w", err)
		}
		// The rows are locked by this transaction, so all of them must have been updated.
		if int(tag.RowsAffected()) != len(due) {
			return fmt.Errorf("mark queued: updated %d of %d rows", tag.RowsAffected(), len(due))
		}
		n = len(due)
		return nil
	})
	return n, err
}

// ReapExpiredLeases moves up to limit processing notifications whose lease expired (their worker
// crashed or stalled) to retrying, due immediately (ADR-0003). The attempt stays counted.
func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	t := transition{from: []domain.Status{domain.StatusProcessing}, to: domain.StatusRetrying}
	if err := t.validate(); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, `
WITH prev AS (
    SELECT id, created_at, status FROM notifications
     WHERE status = 'processing' AND lease_until < now()
     ORDER BY lease_until
     LIMIT $1
       FOR UPDATE SKIP LOCKED
), upd AS (
    UPDATE notifications n
       SET status = 'retrying', next_attempt_at = now(), lease_until = NULL, lease_token = NULL,
           last_error = 'lease expired', updated_at = now()
      FROM prev
     WHERE n.id = prev.id AND n.created_at = prev.created_at
 RETURNING n.id, n.created_at, n.attempts, prev.status AS prev_status
)
INSERT INTO notification_status_history
       (notification_id, notification_created_at, from_status, to_status, attempt, reason)
SELECT id, created_at, prev_status, 'retrying', attempts, 'lease expired' FROM upd`, limit)
	if err != nil {
		return 0, fmt.Errorf("reap expired leases: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// RequeueStuck moves up to limit notifications that have been queued for longer than olderThan
// back to pending, so the dispatcher publishes them again. Not wired to any loop yet: the reaper
// uses it only as the long age fallback (default 24 h). Its main rule compares stored Kafka
// offsets with the consumer group's committed offsets (ADR-0003 amendment), added in Phase 5.
func (s *Store) RequeueStuck(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	t := transition{from: []domain.Status{domain.StatusQueued}, to: domain.StatusPending}
	if err := t.validate(); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, `
WITH prev AS (
    SELECT id, created_at, status FROM notifications
     WHERE status = 'queued' AND queued_at < now() - make_interval(secs => $1)
     ORDER BY queued_at
     LIMIT $2
       FOR UPDATE SKIP LOCKED
), upd AS (
    UPDATE notifications n
       SET status = 'pending', next_attempt_at = now(), queued_at = NULL, updated_at = now()
      FROM prev
     WHERE n.id = prev.id AND n.created_at = prev.created_at
 RETURNING n.id, n.created_at, n.attempts, prev.status AS prev_status
)
INSERT INTO notification_status_history
       (notification_id, notification_created_at, from_status, to_status, attempt, reason)
SELECT id, created_at, prev_status, 'pending', attempts, 'stuck in queued' FROM upd`, olderThan.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("requeue stuck notifications: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func notifyDue(ctx context.Context, q querier) error {
	// Delivered when the transaction commits, and dropped if it rolls back.
	if _, err := q.Exec(ctx, `SELECT pg_notify($1, '')`, NotifyChannelDue); err != nil {
		return fmt.Errorf("notify %s: %w", NotifyChannelDue, err)
	}
	return nil
}

// Package storage is the PostgreSQL persistence layer (ADR-0001, ADR-0003, ADR-0004, ADR-0008).
//
// Every status change is a guarded UPDATE (`WHERE status = ANY(<allowed sources>)`) that appends
// a notification_status_history row in the same statement, so the current state and its audit
// trail can never diverge. Allowed sources are checked against the domain state machine before
// any SQL runs.
package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// NotifyChannelDue is the LISTEN/NOTIFY channel signalled when a notification becomes due now
// (ADR-0003). The dispatcher listens on it; a polling fallback covers scheduled and retry rows.
const NotifyChannelDue = "notifications_due"

// Options configures a Store.
type Options struct {
	// IdempotencyTTL is how long an Idempotency-Key is remembered (ADR-0004). Default 72h.
	IdempotencyTTL time.Duration
	// Now returns the current time. It sets ids and created_at. Default time.Now.
	Now func() time.Time
}

// Store implements persistence on a pgx pool. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
	ttl  time.Duration
	now  func() time.Time
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool, opts Options) *Store {
	if opts.IdempotencyTTL <= 0 {
		opts.IdempotencyTTL = 72 * time.Hour
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Store{pool: pool, ttl: opts.IdempotencyTTL, now: opts.Now}
}

// querier is satisfied by *pgxpool.Pool and pgx.Tx.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// notificationColumns is the column list scanned by scanNotification, in order.
var notificationColumns = []string{
	"id", "created_at", "batch_id", "channel", "priority", "recipient", "content",
	"status", "attempts", "next_attempt_at", "scheduled_at", "queued_at", "lease_until",
	"lease_token", "sent_at", "provider_message_id", "last_error", "correlation_id", "updated_at",
}

// columns returns notificationColumns, each prefixed with "alias." when alias is not empty.
func columns(alias string) string {
	if alias == "" {
		return strings.Join(notificationColumns, ", ")
	}
	return alias + "." + strings.Join(notificationColumns, ", "+alias+".")
}

func scanNotification(row pgx.Row) (domain.Notification, error) {
	var (
		n                         domain.Notification
		channel, priority, status string
	)
	err := row.Scan(
		&n.ID, &n.CreatedAt, &n.BatchID, &channel, &priority, &n.Recipient, &n.Content,
		&status, &n.Attempts, &n.NextAttemptAt, &n.ScheduledAt, &n.QueuedAt, &n.LeaseUntil,
		&n.LeaseToken, &n.SentAt, &n.ProviderMessageID, &n.LastError, &n.CorrelationID, &n.UpdatedAt,
	)
	if err != nil {
		return domain.Notification{}, err
	}
	n.Channel, n.Priority, n.Status = domain.Channel(channel), domain.Priority(priority), domain.Status(status)
	n.CreatedAt, n.NextAttemptAt, n.UpdatedAt = n.CreatedAt.UTC(), n.NextAttemptAt.UTC(), n.UpdatedAt.UTC()
	for _, t := range []*time.Time{n.ScheduledAt, n.QueuedAt, n.LeaseUntil, n.SentAt} {
		if t != nil {
			*t = t.UTC()
		}
	}
	return n, nil
}

// GetNotification returns the notification with the given id (FR-1.3).
func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return getNotification(ctx, s.pool, id)
}

func getNotification(ctx context.Context, q querier, id uuid.UUID) (domain.Notification, error) {
	if !domain.IsV7(id) {
		return domain.Notification{}, domain.ErrNotFound
	}
	// created_at is derived from the id, so only one partition is read.
	n, err := scanNotification(q.QueryRow(ctx,
		`SELECT `+columns("")+` FROM notifications WHERE id = $1 AND created_at = $2`,
		id, domain.IDTime(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return n, nil
}

// History returns the status changes of a notification, oldest first.
func (s *Store) History(ctx context.Context, id uuid.UUID) ([]domain.StatusChange, error) {
	if !domain.IsV7(id) {
		return nil, domain.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `
		SELECT from_status, to_status, attempt, reason, created_at
		  FROM notification_status_history
		 WHERE notification_id = $1 AND notification_created_at = $2
		 ORDER BY id`, id, domain.IDTime(id))
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	changes, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.StatusChange, error) {
		var (
			c        domain.StatusChange
			from     *string
			to       string
			occurred time.Time
		)
		if err := r.Scan(&from, &to, &c.Attempt, &c.Reason, &occurred); err != nil {
			return c, err
		}
		if from != nil {
			f := domain.Status(*from)
			c.From = &f
		}
		c.To, c.At = domain.Status(to), occurred.UTC()
		return c, nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan history: %w", err)
	}
	if len(changes) == 0 {
		return nil, domain.ErrNotFound
	}
	return changes, nil
}

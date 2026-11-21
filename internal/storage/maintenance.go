package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Daily range partitions (ADR-0008). Each day D has notifications_pYYYYMMDD and
// notification_status_history_pYYYYMMDD covering [D 00:00 UTC, D+1 00:00 UTC). History is
// partitioned by the parent notification's created_at, so both are dropped together.
//
// There is deliberately no DEFAULT partition: it would make every later CREATE ... PARTITION OF
// scan it, and it blocks DETACH ... CONCURRENTLY. Partitions are created days ahead instead.

const (
	partitionDateLayout = "20060102"
	// notificationsFillfactor leaves 20% of every heap page free, so the new row version written
	// by a status update usually fits on the same page. That keeps a notification's versions
	// together and lets pruning reclaim space without vacuum (ADR-0008).
	notificationsFillfactor = 80
	// maintenanceLockID is the advisory lock that makes partition maintenance single-flight
	// across dispatcher replicas.
	maintenanceLockID = 0x6e6f7469 // "noti"
)

var partitionedTables = []struct {
	parent     string
	fillfactor int
}{
	{"notifications", notificationsFillfactor},
	{"notification_status_history", 0}, // append-only: default fillfactor (100)
}

var partitionName = regexp.MustCompile(`^(notifications|notification_status_history)_p(\d{8})$`)

// EnsurePartitions creates the daily partitions for days [from, from+days) that do not exist
// yet. It is idempotent and waits for the maintenance lock, so it can run while dispatchers
// maintain partitions (used by `notifier migrate`).
func (s *Store) EnsurePartitions(ctx context.Context, from time.Time, days int) error {
	return withMaintenanceConn(ctx, s.pool, true, func(conn *pgxpool.Conn) error {
		return ensurePartitions(ctx, conn, from, days)
	})
}

func ensurePartitions(ctx context.Context, conn *pgxpool.Conn, from time.Time, days int) error {
	start := truncateDay(from)
	for d := range days {
		lo := start.AddDate(0, 0, d)
		hi := lo.AddDate(0, 0, 1)
		for _, t := range partitionedTables {
			sql := fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
				pgx.Identifier{t.parent + "_p" + lo.Format(partitionDateLayout)}.Sanitize(),
				pgx.Identifier{t.parent}.Sanitize(),
				lo.Format(time.RFC3339), hi.Format(time.RFC3339))
			if t.fillfactor > 0 {
				sql += fmt.Sprintf(` WITH (fillfactor = %d)`, t.fillfactor)
			}
			if _, err := conn.Exec(ctx, sql); err != nil {
				return fmt.Errorf("create partition of %s for %s: %w", t.parent, lo.Format(time.DateOnly), err)
			}
		}
	}
	return nil
}

// DropPartitionsBefore drops every daily partition that ends on or before cutoff (rounded down
// to midnight UTC) and returns the names of the dropped tables. Dropping a partition is instant
// and leaves nothing for vacuum, unlike a bulk DELETE.
//
// Partitions are detached with DETACH ... CONCURRENTLY, which does not block reads and writes
// on the parent, and then dropped. A detach interrupted earlier is finished first.
func (s *Store) DropPartitionsBefore(ctx context.Context, cutoff time.Time) ([]string, error) {
	var dropped []string
	err := withMaintenanceConn(ctx, s.pool, true, func(conn *pgxpool.Conn) error {
		var err error
		dropped, err = dropPartitionsBefore(ctx, conn, cutoff)
		return err
	})
	return dropped, err
}

func dropPartitionsBefore(ctx context.Context, conn *pgxpool.Conn, cutoff time.Time) ([]string, error) {
	cutoffDay := truncateDay(cutoff)
	rows, err := conn.Query(ctx, `
		SELECT c.relname, i.inhrelid IS NOT NULL, coalesce(i.inhdetachpending, false)
		  FROM pg_class c
		  LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
		 WHERE c.relkind = 'r'
		   AND c.relnamespace = current_schema()::regnamespace
		   AND c.relname ~ '^(notifications|notification_status_history)_p[0-9]{8}$'
		 ORDER BY c.relname`)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	type partition struct { // exported fields: pgx sets them by reflection
		Name                    string
		Attached, DetachPending bool
	}
	parts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[partition])
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}

	var dropped []string
	for _, p := range parts {
		m := partitionName.FindStringSubmatch(p.Name)
		if m == nil {
			continue
		}
		day, err := time.Parse(partitionDateLayout, m[2])
		if err != nil || day.AddDate(0, 0, 1).After(cutoffDay) {
			continue
		}
		table, parent := pgx.Identifier{p.Name}.Sanitize(), pgx.Identifier{m[1]}.Sanitize()
		var stmts []string
		switch {
		case p.DetachPending:
			stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s FINALIZE`, parent, table))
		case p.Attached:
			stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s CONCURRENTLY`, parent, table))
		}
		stmts = append(stmts, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
		for _, stmt := range stmts {
			// DETACH ... CONCURRENTLY cannot run inside a transaction block, so use the simple
			// protocol (one implicit transaction per statement).
			if _, err := conn.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol); err != nil {
				return dropped, fmt.Errorf("drop partition %s: %w", p.Name, err)
			}
		}
		dropped = append(dropped, p.Name)
	}
	return dropped, nil
}

// MaintenanceOptions configures Maintain.
type MaintenanceOptions struct {
	// PremakeDays is how many days of partitions to keep ready ahead of today.
	PremakeDays int
	// RetentionDays is how many whole days of notifications (and batches) to keep.
	RetentionDays int
	// CleanupLimit bounds how many expired idempotency keys and old batches one run deletes.
	CleanupLimit int
}

// MaintenanceReport describes what Maintain did.
type MaintenanceReport struct {
	// Skipped is true when another process held the maintenance lock.
	Skipped        bool
	DroppedTables  []string
	DeletedKeys    int
	DeletedBatches int
}

// Maintain performs the data lifecycle tasks (ADR-0008): it creates partitions for yesterday
// through PremakeDays ahead, drops partitions and batches older than RetentionDays and deletes
// expired idempotency keys. A session advisory lock makes it run on one process at a time; the
// others skip.
func (s *Store) Maintain(ctx context.Context, now time.Time, opts MaintenanceOptions) (MaintenanceReport, error) {
	var rep MaintenanceReport
	cutoff := now.AddDate(0, 0, -opts.RetentionDays)
	err := withMaintenanceConn(ctx, s.pool, false, func(conn *pgxpool.Conn) error {
		if err := ensurePartitions(ctx, conn, now.AddDate(0, 0, -1), opts.PremakeDays+2); err != nil {
			return err
		}
		var err error
		if rep.DroppedTables, err = dropPartitionsBefore(ctx, conn, cutoff); err != nil {
			return err
		}
		if rep.DeletedBatches, err = s.deleteBatchesBefore(ctx, truncateDay(cutoff), opts.CleanupLimit); err != nil {
			return err
		}
		rep.DeletedKeys, err = s.DeleteExpiredIdempotencyKeys(ctx, opts.CleanupLimit)
		return err
	})
	if errors.Is(err, errLockBusy) {
		return MaintenanceReport{Skipped: true}, nil
	}
	return rep, err
}

var errLockBusy = errors.New("advisory lock held by another session")

// ddlLockTimeout bounds how long partition DDL waits for locks. CREATE ... PARTITION OF takes an
// ACCESS EXCLUSIVE lock on the parent, and every query on notifications would queue behind a
// waiting DDL statement; giving up quickly and retrying on the next run is harmless because
// partitions are created days ahead.
const ddlLockTimeout = "2s"

// withMaintenanceConn runs fn on a dedicated connection that holds the maintenance advisory lock
// and has lock_timeout set. With wait it blocks until the lock is free (bounded by ctx);
// otherwise it returns errLockBusy if another session holds it. Advisory locks are re-entrant
// per session, so nested use on the same connection is safe.
func withMaintenanceConn(ctx context.Context, pool *pgxpool.Pool, wait bool, fn func(*pgxpool.Conn) error) (err error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if wait {
		if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, maintenanceLockID); err != nil {
			return fmt.Errorf("advisory lock: %w", err)
		}
	} else {
		var locked bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, maintenanceLockID).Scan(&locked); err != nil {
			return fmt.Errorf("try advisory lock: %w", err)
		}
		if !locked {
			return errLockBusy
		}
	}
	defer func() {
		// Reset the session even if ctx was cancelled. If that fails, close the connection: that
		// releases the lock and keeps the setting from leaking into the pool.
		resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, rerr := conn.Exec(resetCtx, `SELECT pg_advisory_unlock($1)`, maintenanceLockID); rerr != nil {
			_ = conn.Conn().Close(resetCtx)
			err = errors.Join(err, fmt.Errorf("advisory unlock: %w", rerr))
			return
		}
		if _, rerr := conn.Exec(resetCtx, `RESET lock_timeout`); rerr != nil {
			_ = conn.Conn().Close(resetCtx)
		}
	}()
	if _, err := conn.Exec(ctx, `SET lock_timeout = '`+ddlLockTimeout+`'`); err != nil {
		return fmt.Errorf("set lock_timeout: %w", err)
	}
	return fn(conn)
}

func truncateDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// DeleteExpiredIdempotencyKeys deletes up to limit expired keys and returns how many it deleted.
//
// The expiry is re-checked in the outer WHERE: a key that a concurrent request takes over after
// the subquery picked it gets a new expires_at, and READ COMMITTED only re-evaluates the outer
// condition on the new row version. SKIP LOCKED keeps the cleanup from waiting on such claims.
func (s *Store) DeleteExpiredIdempotencyKeys(ctx context.Context, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_keys
		 WHERE expires_at <= now()
		   AND key IN (SELECT key FROM idempotency_keys
		                WHERE expires_at <= now()
		                LIMIT $1
		                  FOR UPDATE SKIP LOCKED)`, limit)
	if err != nil {
		return 0, fmt.Errorf("delete expired idempotency keys: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// deleteBatchesBefore deletes up to limit batches created before cutoff. Their notifications
// were dropped with their partitions.
func (s *Store) deleteBatchesBefore(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM batches
		 WHERE id IN (SELECT id FROM batches WHERE created_at < $1 LIMIT $2)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old batches: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

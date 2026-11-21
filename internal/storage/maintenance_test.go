package storage

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/migrations"
)

func partitions(t *testing.T) []string {
	t.Helper()
	rows, err := testPool.Query(context.Background(), `
		SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		 ORDER BY c.relname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(n, "_p") {
			names = append(names, n)
		}
	}
	return names
}

func TestEnsureAndDropPartitions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	old := time.Date(2020, 1, 1, 15, 0, 0, 0, time.UTC)

	if err := s.EnsurePartitions(ctx, old, 3); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePartitions(ctx, old, 3); err != nil {
		t.Fatalf("EnsurePartitions must be idempotent: %v", err)
	}
	got := partitions(t)
	for _, want := range []string{"notifications_p20200101", "notifications_p20200103", "notification_status_history_p20200102"} {
		if !slices.Contains(got, want) {
			t.Errorf("missing %s in %v", want, got)
		}
	}

	var fillfactor string
	if err := testPool.QueryRow(ctx,
		`SELECT array_to_string(reloptions, ',') FROM pg_class WHERE relname = 'notifications_p20200101'`,
	).Scan(&fillfactor); err != nil || fillfactor != "fillfactor=80" {
		t.Errorf("reloptions = %q, %v; want fillfactor=80", fillfactor, err)
	}

	// Row routing: a row created in 2020 lands in its day's partition.
	id := insertRaw(t, old)
	var table string
	if err := testPool.QueryRow(ctx, `SELECT tableoid::regclass::text FROM notifications WHERE id = $1`, id).Scan(&table); err != nil || table != "notifications_p20200101" {
		t.Errorf("row stored in %q, %v", table, err)
	}

	// Cutoff 2020-01-03 12:00 drops the partitions for Jan 1 and Jan 2, not Jan 3.
	dropped, err := s.DropPartitionsBefore(ctx, time.Date(2020, 1, 3, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(dropped)
	want := []string{
		"notification_status_history_p20200101", "notification_status_history_p20200102",
		"notifications_p20200101", "notifications_p20200102",
	}
	if !slices.Equal(dropped, want) {
		t.Errorf("dropped %v, want %v", dropped, want)
	}
	if got := partitions(t); slices.Contains(got, "notifications_p20200101") || !slices.Contains(got, "notifications_p20200103") {
		t.Errorf("partitions after drop: %v", got)
	}
	if count(t, "notifications") != 0 {
		t.Error("the 2020 row should have been dropped with its partition")
	}
}

func TestDropRemovesAnOrphanedDetachedPartition(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	day := time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
	if err := s.EnsurePartitions(ctx, day, 1); err != nil {
		t.Fatal(err)
	}
	// A detached-but-not-dropped table (e.g. the process died between DETACH and DROP).
	if _, err := testPool.Exec(ctx, `ALTER TABLE notifications DETACH PARTITION notifications_p20200201`); err != nil {
		t.Fatal(err)
	}
	dropped, err := s.DropPartitionsBefore(ctx, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(dropped, "notifications_p20200201") {
		t.Errorf("orphaned table not dropped: %v", dropped)
	}
}

func TestMaintainIsSingleFlight(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	opts := MaintenanceOptions{PremakeDays: 3, RetentionDays: 30, CleanupLimit: 100}

	// Another session holds the lock: Maintain skips.
	conn, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, maintenanceLockID); err != nil {
		t.Fatal(err)
	}
	rep, err := s.Maintain(ctx, time.Now(), opts)
	if err != nil || !rep.Skipped {
		t.Fatalf("Maintain with a held lock: %+v, %v", rep, err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, maintenanceLockID); err != nil {
		t.Fatal(err)
	}
	conn.Release()

	now := time.Now().UTC()
	rep, err = s.Maintain(ctx, now, opts)
	if err != nil || rep.Skipped {
		t.Fatalf("Maintain: %+v, %v", rep, err)
	}
	ahead := "notifications_p" + now.AddDate(0, 0, 3).Format(partitionDateLayout)
	if !slices.Contains(partitions(t), ahead) {
		t.Errorf("partition %s was not pre-created", ahead)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	if testPool == nil {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := Migrate(context.Background(), testPool, migrations.FS, logger); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if err := MigrationStatus(context.Background(), testPool, migrations.FS, logger); err != nil {
		t.Fatalf("status: %v", err)
	}
}

// TestLookupsArePrunedToOnePartition checks the point of deriving created_at from the UUIDv7:
// a lookup by id alone reads a single daily partition (ADR-0008).
func TestLookupsArePrunedToOnePartition(t *testing.T) {
	newTestStore(t)
	id := insertRaw(t, time.Now())

	rows, err := testPool.Query(context.Background(),
		`EXPLAIN (COSTS OFF) SELECT 1 FROM notifications WHERE id = $1 AND created_at = $2`,
		id, domain.IDTime(id))
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	if got := strings.Count(plan, " on notifications_p"); got != 1 {
		t.Errorf("plan scans %d partitions, want 1:\n%s", got, plan)
	}
}

// insertRaw stores a minimal notification created at t without going through the Store.
func insertRaw(t *testing.T, at time.Time) uuid.UUID {
	t.Helper()
	id := domain.NewIDAt(at)
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO notifications (id, created_at, channel, priority, recipient, content, next_attempt_at)
		VALUES ($1, $2, 'sms', 'normal', '+905551112233', 'x', now())`, id, domain.IDTime(id)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id
}

func TestMaintainDeletesOldBatches(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	old, recent := domain.NewIDAt(now.AddDate(0, 0, -40)), domain.NewIDAt(now)
	for _, id := range []uuid.UUID{old, recent} {
		if _, err := testPool.Exec(ctx, `INSERT INTO batches (id, created_at, total) VALUES ($1, $2, 1)`,
			id, domain.IDTime(id)); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := s.Maintain(ctx, now, MaintenanceOptions{PremakeDays: 1, RetentionDays: 30, CleanupLimit: 100})
	if err != nil || rep.DeletedBatches != 1 {
		t.Fatalf("report %+v, err %v; want 1 deleted batch", rep, err)
	}
	if count(t, "batches") != 1 {
		t.Error("the recent batch must be kept")
	}
}

func TestEnsurePartitionsWaitsForTheMaintenanceLock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	conn, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, maintenanceLockID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.EnsurePartitions(ctx, time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC), 1) }()
	select {
	case err := <-done:
		t.Fatalf("EnsurePartitions did not wait for the lock (err %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, maintenanceLockID); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("EnsurePartitions after unlock: %v", err)
	}
}

func TestMaintenanceConnectionIsResetBeforeReuse(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.EnsurePartitions(ctx, time.Now(), 1); err != nil {
		t.Fatal(err)
	}
	// Every pooled connection must be back to the default lock_timeout (0) and hold no lock.
	for range 5 {
		var timeout string
		var held int
		if err := testPool.QueryRow(ctx, `SELECT current_setting('lock_timeout'),
			(SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid())`).Scan(&timeout, &held); err != nil {
			t.Fatal(err)
		}
		if timeout != "0" || held != 0 {
			t.Fatalf("connection leaked lock_timeout=%s advisory locks=%d", timeout, held)
		}
	}
}

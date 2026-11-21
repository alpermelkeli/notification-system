package storage

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alpermelkeli/notification-system/internal/testinfra"
	"github.com/alpermelkeli/notification-system/migrations"
)

// testPool is a migrated PostgreSQL 17 shared by the integration tests of this package. It is
// nil under -short, and tests that need it skip.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	ctx := context.Background()
	pool, stop, err := testinfra.Postgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer stop()
	if err := Migrate(ctx, pool, migrations.FS, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	testPool = pool
	return m.Run()
}

// newTestStore returns a Store with default options on an empty database with partitions
// around now. Tests may adjust its unexported fields (clock, key TTL).
func newTestStore(t *testing.T) *Store {
	t.Helper()
	if testPool == nil {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	ctx := context.Background()
	if _, err := testPool.Exec(ctx,
		`TRUNCATE notifications, notification_status_history, batches, idempotency_keys, templates`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	s := New(testPool, Options{})
	if err := s.EnsurePartitions(ctx, time.Now().AddDate(0, 0, -2), 5); err != nil {
		t.Fatalf("ensure partitions: %v", err)
	}
	return s
}

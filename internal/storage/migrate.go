package storage

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// newMigrator returns a goose provider for the migrations in fsys. A Postgres session lock makes
// concurrent `notifier migrate` runs wait for each other instead of racing.
func newMigrator(pool *pgxpool.Pool, fsys fs.FS) (*goose.Provider, func() error, error) {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, nil, fmt.Errorf("create migration lock: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)
	p, err := goose.NewProvider(goose.DialectPostgres, db, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("create migration provider: %w", err)
	}
	return p, db.Close, nil
}

// Migrate applies all pending migrations from fsys (D-5) and logs each one.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, logger *slog.Logger) error {
	p, closeDB, err := newMigrator(pool, fsys)
	if err != nil {
		return err
	}
	defer func() { _ = closeDB() }()

	results, err := p.Up(ctx)
	for _, r := range results {
		logger.Info("migration applied", "version", r.Source.Version, "file", r.Source.Path, "duration_ms", r.Duration.Milliseconds())
	}
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	version, err := p.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	logger.Info("schema up to date", "version", version, "applied", len(results))
	return nil
}

// MigrationStatus logs the state of every known migration.
func MigrationStatus(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, logger *slog.Logger) error {
	p, closeDB, err := newMigrator(pool, fsys)
	if err != nil {
		return err
	}
	defer func() { _ = closeDB() }()

	statuses, err := p.Status(ctx)
	if err != nil {
		return fmt.Errorf("migration status: %w", err)
	}
	for _, s := range statuses {
		logger.Info("migration", "version", s.Source.Version, "file", s.Source.Path,
			"state", string(s.State), "applied_at", s.AppliedAt)
	}
	return nil
}

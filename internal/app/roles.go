package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/alpermelkeli/notification-system/internal/config"
	"github.com/alpermelkeli/notification-system/internal/health"
	"github.com/alpermelkeli/notification-system/internal/platform"
	"github.com/alpermelkeli/notification-system/internal/storage"
	"github.com/alpermelkeli/notification-system/migrations"
)

// RunMigrate applies pending schema migrations and creates the daily partitions for yesterday
// through PartitionPremakeDays ahead (D-5). docker-compose runs it as a one-shot job before the
// other roles start.
func RunMigrate(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := storage.Migrate(ctx, pool, migrations.FS, logger); err != nil {
		return err
	}
	store := storage.New(pool, storage.Options{})
	if err := store.EnsurePartitions(ctx, time.Now().AddDate(0, 0, -1), cfg.PartitionPremakeDays+2); err != nil {
		return err
	}
	logger.Info("partitions ready", "premake_days", cfg.PartitionPremakeDays)
	return nil
}

// RunMigrateStatus logs the state of every migration.
func RunMigrateStatus(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	return storage.MigrationStatus(ctx, pool, migrations.FS, logger)
}

// RunAPI serves the public HTTP API on cfg.HTTPAddr until ctx is cancelled.
func RunAPI(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	router := baseRouter(logger, cfg.InstanceID, []health.Check{platform.PostgresCheck(pool)})
	// The /v1 notification endpoints are mounted here in Phase 4.

	return serveHTTP(ctx, logger, cfg.HTTPAddr, router, cfg.ShutdownTimeout)
}

// RunDispatcher relays due notifications from PostgreSQL to Kafka (ADR-0003). It serves health
// and metrics on cfg.AdminAddr.
func RunDispatcher(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	kafka, err := platform.NewKafkaClient(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer kafka.Close()

	store := storage.New(pool, storage.Options{IdempotencyTTL: cfg.IdempotencyTTL})
	var wg sync.WaitGroup
	defer wg.Wait() // background loops use the pool, which is closed by an earlier defer
	// Stop the background loops when this function returns for any reason, e.g. the admin
	// server failing to listen; deferred after wg.Wait, so it runs first.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Go(func() { runMaintenance(ctx, store, cfg, logger) })
	// The relay loop and reaper are started here in Phase 5.

	checks := []health.Check{platform.PostgresCheck(pool), platform.KafkaCheck(kafka)}
	return serveAdmin(ctx, cfg, logger, checks)
}

// RunWorker consumes the channel topics and delivers notifications (ADR-0002, ADR-0006). It
// serves health and metrics on cfg.AdminAddr.
func RunWorker(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	kafka, err := platform.NewKafkaClient(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer kafka.Close()

	rdb := platform.NewRedisClient(cfg.RedisAddr, cfg.RedisPassword)
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.Warn("close redis client", "err", err)
		}
	}()

	checks := []health.Check{
		platform.PostgresCheck(pool), platform.KafkaCheck(kafka), platform.RedisCheck(rdb),
	}
	// The per-channel consumers, scheduler and senders are started here in Phase 5.
	return serveAdmin(ctx, cfg, logger, checks)
}

// runMaintenance runs the data lifecycle (partitions, retention, expired idempotency keys) at
// start-up and then every MaintenanceInterval until ctx is cancelled. Every dispatcher replica
// runs this loop; an advisory lock lets only one of them work at a time (ADR-0008).
func runMaintenance(ctx context.Context, store *storage.Store, cfg config.Config, logger *slog.Logger) {
	opts := storage.MaintenanceOptions{
		PremakeDays:   cfg.PartitionPremakeDays,
		RetentionDays: cfg.RetentionDays,
		CleanupLimit:  10_000,
	}
	ticker := time.NewTicker(cfg.MaintenanceInterval)
	defer ticker.Stop()
	for {
		rep, err := store.Maintain(ctx, time.Now(), opts)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			// The next run retries; partitions are pre-created days ahead, so a missed run is safe.
			logger.Error("maintenance failed", "err", err)
		case rep.Skipped:
			logger.Debug("maintenance skipped: another instance holds the lock")
		default:
			logger.Info("maintenance done", "dropped_tables", rep.DroppedTables,
				"deleted_keys", rep.DeletedKeys, "deleted_batches", rep.DeletedBatches)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func serveAdmin(ctx context.Context, cfg config.Config, logger *slog.Logger, checks []health.Check) error {
	if err := serveHTTP(ctx, logger, cfg.AdminAddr, baseRouter(logger, cfg.InstanceID, checks), cfg.ShutdownTimeout); err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	return nil
}

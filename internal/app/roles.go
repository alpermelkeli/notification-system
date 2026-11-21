package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/api"
	"github.com/alpermelkeli/notification-system/internal/config"
	"github.com/alpermelkeli/notification-system/internal/dispatcher"
	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/health"
	"github.com/alpermelkeli/notification-system/internal/platform"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/ratelimit"
	"github.com/alpermelkeli/notification-system/internal/retry"
	"github.com/alpermelkeli/notification-system/internal/storage"
	"github.com/alpermelkeli/notification-system/internal/worker"
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
	store := storage.New(pool, storage.Options{IdempotencyTTL: cfg.IdempotencyTTL})
	api.New(store, logger, api.Options{
		MaxScheduleAhead: cfg.MaxScheduleAhead, RequestTimeout: cfg.RequestTimeout,
	}).Routes(router)

	return serveHTTP(ctx, logger, cfg.HTTPAddr, router, cfg.ShutdownTimeout)
}

// RunDispatcher relays due notifications from PostgreSQL to Kafka, recovers notifications that
// never reach a worker and runs data maintenance (ADR-0003, ADR-0008). Any number of replicas can
// run. It serves health and metrics on cfg.AdminAddr.
func RunDispatcher(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	kafka, err := platform.NewKafkaClient(cfg.KafkaBrokers,
		append(dispatcher.ProducerOptions(), kgo.ClientID("dispatcher-"+cfg.InstanceID))...)
	if err != nil {
		return err
	}
	defer kafka.Close()

	store := storage.New(pool, storage.Options{IdempotencyTTL: cfg.IdempotencyTTL})
	relay := dispatcher.NewRelay(store, dispatcher.NewPublisher(kafka, nil).Publish, logger, dispatcher.RelayOptions{
		BatchSize: cfg.Dispatcher.BatchSize, PollInterval: cfg.Dispatcher.PollInterval,
	})
	dead := queue.NewDeadLetters(kafka, dispatcher.PublishTimeout)
	reaper := dispatcher.NewReaper(store, dispatcher.NewKafkaOffsets(kadm.NewClient(kafka)), dead, logger, dispatcher.ReaperOptions{
		Interval: cfg.Dispatcher.ReaperInterval, Grace: cfg.Dispatcher.ReaperGrace,
		StuckAfter: cfg.Dispatcher.ReaperStuckAfter, MaxAttempts: cfg.Retry.MaxAttempts,
	})

	var wg sync.WaitGroup
	defer wg.Wait() // background loops use the pool and the client, which earlier defers close
	// Stop the background loops when this function returns for any reason, e.g. the admin
	// server failing to listen; deferred after wg.Wait, so it runs first.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() { reaper.Run(ctx) })
	wg.Go(func() { runMaintenance(ctx, store, cfg, logger) })

	checks := []health.Check{platform.PostgresCheck(pool), platform.KafkaCheck(kafka)}
	return serveAdmin(ctx, cfg, logger, checks)
}

// RunWorker delivers notifications of cfg.Worker.Channels (ADR-0002, ADR-0006): one consumer
// group member per channel, sharing the database pool, the provider client and the Redis rate
// limiter. It serves health and metrics on cfg.AdminAddr.
func RunWorker(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	rdb := platform.NewRedisClient(cfg.RedisAddr, cfg.RedisPassword)
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.Warn("close redis client", "err", err)
		}
	}()

	sender, err := provider.New(cfg.Worker.ProviderURL, cfg.Worker.ProviderTimeout)
	if err != nil {
		return err
	}
	limits := map[domain.Channel]ratelimit.Limit{}
	for c, perSecond := range cfg.Worker.RateLimits {
		limits[c] = ratelimit.Limit{PerSecond: perSecond, Burst: cfg.Worker.RateLimitBurst}
	}
	limiter, err := ratelimit.New(rdb, sender.Name(), limits)
	if err != nil {
		return err
	}
	// Dead letters go through their own producer: the consumers' clients are per channel and leave
	// their group on shutdown.
	producer, err := platform.NewKafkaClient(cfg.KafkaBrokers,
		append(dispatcher.ProducerOptions(), kgo.ClientID("worker-dlq-"+cfg.InstanceID))...)
	if err != nil {
		return err
	}
	defer producer.Close()

	store := storage.New(pool, storage.Options{IdempotencyTTL: cfg.IdempotencyTTL})
	policy := worker.DefaultPolicy()
	policy.AgingRate, policy.MinShareEvery = cfg.Worker.AgingRate, cfg.Worker.MinShareEvery
	opts := worker.Options{
		Concurrency: cfg.Worker.Concurrency, Lease: cfg.Worker.Lease,
		Policy: &policy, DeadLetters: queue.NewDeadLetters(producer, dispatcher.PublishTimeout),
		Retry: retry.Policy{
			BaseDelay: cfg.Retry.BaseDelay, MaxDelay: cfg.Retry.MaxDelay, MaxAttempts: cfg.Retry.MaxAttempts,
		},
		Breaker: worker.BreakerOptions(cfg.Worker.Breaker),
	}

	var wg sync.WaitGroup
	defer wg.Wait() // consumers use the pool and Redis, which earlier defers close
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	checks := []health.Check{platform.PostgresCheck(pool), platform.RedisCheck(rdb)}
	for _, c := range cfg.Worker.Channels {
		w := worker.New(c, store, sender, limiter, logger, opts)
		cl, err := platform.NewKafkaClient(cfg.KafkaBrokers,
			append(w.ConsumerOptions(), kgo.ClientID("worker-"+string(c)+"-"+cfg.InstanceID))...)
		if err != nil {
			return err
		}
		if len(checks) == 2 {
			checks = append(checks, platform.KafkaCheck(cl))
		}
		wg.Go(func() {
			w.Run(ctx, cl)
			cl.CloseAllowingRebalance() // leaves the group after in-flight sends are persisted and committed
		})
	}
	logger.Info("worker started", "channels", cfg.Worker.Channels, "provider", sender.Name())
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

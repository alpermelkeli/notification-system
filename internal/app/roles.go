package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/api"
	"github.com/alpermelkeli/notification-system/internal/config"
	"github.com/alpermelkeli/notification-system/internal/dispatcher"
	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/events"
	"github.com/alpermelkeli/notification-system/internal/health"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/platform"
	"github.com/alpermelkeli/notification-system/internal/playground"
	"github.com/alpermelkeli/notification-system/internal/provider"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/ratelimit"
	"github.com/alpermelkeli/notification-system/internal/retry"
	"github.com/alpermelkeli/notification-system/internal/storage"
	"github.com/alpermelkeli/notification-system/internal/worker"
	"github.com/alpermelkeli/notification-system/internal/ws"
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

	// Status events only (ADR-0011). Kafka is not part of readiness: without it creates and
	// cancels still work and only live updates pause.
	eventClient, err := newEventClient(cfg, "api")
	if err != nil {
		return err
	}
	defer closeProducer(ctx, eventClient, logger)

	// The WebSocket hub reads every status event, without a consumer group (ADR-0011).
	consumer, err := platform.NewKafkaClient(cfg.KafkaBrokers,
		append(ws.ConsumerOptions(), kgo.ClientID("api-events-"+cfg.InstanceID))...)
	if err != nil {
		return err
	}
	defer consumer.Close()

	reg := metrics.NewRegistry()
	router, httpMetrics := baseRouter(logger, cfg.InstanceID, []health.Check{platform.PostgresCheck(pool)}, reg)
	store := storage.New(pool, storage.Options{IdempotencyTTL: cfg.IdempotencyTTL})
	hub := ws.NewHub(metrics.NewWebSocket(reg), logger)
	api.New(store, logger, api.Options{
		MaxScheduleAhead: cfg.MaxScheduleAhead, RequestTimeout: cfg.RequestTimeout, Metrics: httpMetrics,
		Events:    events.NewPublisher(eventClient, metrics.NewEvents(reg), logger),
		WebSocket: ws.NewHandler(hub, store, logger, ws.HandlerOptions{AllowedOrigins: cfg.WSAllowedOrigins}),
	}).Routes(router)
	mountPlayground(router, cfg, logger)

	var wg sync.WaitGroup
	defer wg.Wait() // the consumer loop uses the client, which an earlier defer closes
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Go(func() { hub.Consume(ctx, consumer) })
	// http.Server.Shutdown does not track upgraded connections: close them as soon as the
	// shutdown starts, and wait for their handlers before the pool closes. Shutdown runs again
	// before the wait, so no handler can register while the wait runs.
	wg.Go(func() { <-ctx.Done(); hub.Shutdown() })
	defer func() {
		hub.Shutdown()
		wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer wcancel()
		if err := hub.Wait(wctx); err != nil {
			logger.Warn("websocket connections still open at shutdown", "err", err)
		}
	}()

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
	eventClient, err := newEventClient(cfg, "dispatcher")
	if err != nil {
		return err
	}
	defer closeProducer(ctx, eventClient, logger)

	reg := metrics.NewRegistry()
	m := metrics.NewDispatch(reg)
	evts := events.NewPublisher(eventClient, metrics.NewEvents(reg), logger)
	store := storage.New(pool, storage.Options{IdempotencyTTL: cfg.IdempotencyTTL})
	relay := dispatcher.NewRelay(store, dispatcher.NewPublisher(kafka, m).Publish, logger, dispatcher.RelayOptions{
		BatchSize: cfg.Dispatcher.BatchSize, PollInterval: cfg.Dispatcher.PollInterval, Metrics: m, Events: evts,
	})
	dead := queue.NewDeadLetters(kafka, dispatcher.PublishTimeout)
	offsets := dispatcher.NewKafkaOffsets(kadm.NewClient(kafka))
	reaper := dispatcher.NewReaper(store, offsets, dead, logger, dispatcher.ReaperOptions{
		Interval: cfg.Dispatcher.ReaperInterval, Grace: cfg.Dispatcher.ReaperGrace,
		StuckAfter: cfg.Dispatcher.ReaperStuckAfter, MaxAttempts: cfg.Retry.MaxAttempts, Metrics: m, Events: evts,
	})
	depth := dispatcher.NewDepthMonitor(store, offsets, m, logger, cfg.Dispatcher.DepthInterval)

	var wg sync.WaitGroup
	defer wg.Wait() // background loops use the pool and the client, which earlier defers close
	// Stop the background loops when this function returns for any reason, e.g. the admin
	// server failing to listen; deferred after wg.Wait, so it runs first.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() { reaper.Run(ctx) })
	wg.Go(func() { depth.Run(ctx) })
	wg.Go(func() { runMaintenance(ctx, store, cfg, logger) })

	checks := []health.Check{platform.PostgresCheck(pool), platform.KafkaCheck("kafka", kafka)}
	return serveAdmin(ctx, cfg, logger, checks, reg)
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
	eventClient, err := newEventClient(cfg, "worker")
	if err != nil {
		return err
	}
	defer closeProducer(ctx, eventClient, logger)

	reg := metrics.NewRegistry()
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
		Metrics: metrics.NewDelivery(reg),
		Events:  events.NewPublisher(eventClient, metrics.NewEvents(reg), logger),
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
		// Every channel's consumer has its own client, so readiness checks each of them.
		checks = append(checks, platform.KafkaCheck("kafka-"+string(c), cl))
		wg.Go(func() {
			w.Run(ctx, cl)
			cl.CloseAllowingRebalance() // leaves the group after in-flight sends are persisted and committed
		})
	}
	logger.Info("worker started", "channels", cfg.Worker.Channels, "provider", sender.Name())
	return serveAdmin(ctx, cfg, logger, checks, reg)
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

// newEventClient returns the role's status event producer. It is separate from every other
// producer, so event records can never hold up work records or dead letters (ADR-0011).
func newEventClient(cfg config.Config, role string) (*kgo.Client, error) {
	return platform.NewKafkaClient(cfg.KafkaBrokers,
		append(events.ProducerOptions(), kgo.ClientID(role+"-events-"+cfg.InstanceID))...)
}

// producerFlushTimeout bounds how long shutdown waits for buffered status events before closing
// their client.
const producerFlushTimeout = 5 * time.Second

// closeProducer sends the events still buffered in cl, within producerFlushTimeout, and closes
// it, so the last transitions before a shutdown still reach WebSocket clients. ctx is the role's
// context, which is usually cancelled by then; the flush gets its own deadline.
func closeProducer(ctx context.Context, cl *kgo.Client, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), producerFlushTimeout)
	defer cancel()
	if err := cl.Flush(ctx); err != nil {
		logger.Warn("buffered kafka records not flushed before shutdown", "err", err)
	}
	cl.Close()
}

func serveAdmin(ctx context.Context, cfg config.Config, logger *slog.Logger, checks []health.Check, reg *prometheus.Registry) error {
	router, _ := baseRouter(logger, cfg.InstanceID, checks, reg)
	if err := serveHTTP(ctx, logger, cfg.AdminAddr, router, cfg.ShutdownTimeout); err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	return nil
}

// mountPlayground serves /playground when DEMO_ENABLED is set (ADR-0011). The failure scenarios
// change the mock provider's faults, so they are turned off when the workers deliver somewhere
// else (PROVIDER_URL on another host): the faults would not affect delivery, and the scenarios
// would wait for failures that never come.
func mountPlayground(r chi.Router, cfg config.Config, logger *slog.Logger) {
	if !cfg.Demo.Enabled {
		return
	}
	admin := cfg.Demo.ProviderAdminURL
	if admin != "" && hostOf(admin) != hostOf(cfg.Worker.ProviderURL) {
		logger.Warn("playground failure scenarios off: PROVIDER_URL is not the mock provider",
			"provider_host", hostOf(cfg.Worker.ProviderURL), "admin_host", hostOf(admin))
		admin = ""
	}
	playground.New(logger, playground.Options{PrometheusURL: cfg.Demo.PrometheusURL, ProviderAdminURL: admin}).Routes(r)
	logger.Info("playground enabled", "path", playground.PathPage)
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Host
}

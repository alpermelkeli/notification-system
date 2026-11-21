package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alpermelkeli/notification-system/internal/config"
	"github.com/alpermelkeli/notification-system/internal/health"
	"github.com/alpermelkeli/notification-system/internal/platform"
)

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

	checks := []health.Check{platform.PostgresCheck(pool), platform.KafkaCheck(kafka)}
	// The relay loop and reaper are started here in Phase 5.
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

func serveAdmin(ctx context.Context, cfg config.Config, logger *slog.Logger, checks []health.Check) error {
	if err := serveHTTP(ctx, logger, cfg.AdminAddr, baseRouter(logger, cfg.InstanceID, checks), cfg.ShutdownTimeout); err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	return nil
}

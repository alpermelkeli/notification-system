// Package platform constructs clients for the infrastructure dependencies (PostgreSQL, Kafka,
// Redis) and exposes them as readiness checks.
//
// All constructors are lazy: they validate configuration but do not require the dependency to
// be reachable, so a process starts (and reports "not ready") during an outage instead of
// crash-looping.
package platform

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/health"
)

// NewPostgresPool parses the connection string and creates a pgx pool.
func NewPostgresPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	return pool, nil
}

// PostgresCheck pings the pool.
func PostgresCheck(pool *pgxpool.Pool) health.Check {
	return health.Check{Name: "postgres", Probe: pool.Ping}
}

// NewKafkaClient creates a franz-go client. Callers add role-specific options (producer
// settings, consumer group, ...).
func NewKafkaClient(brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	all := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return cl, nil
}

// KafkaCheck pings a broker.
func KafkaCheck(cl *kgo.Client) health.Check {
	return health.Check{Name: "kafka", Probe: cl.Ping}
}

// NewRedisClient creates a go-redis client.
func NewRedisClient(addr, password string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr, Password: password})
}

// RedisCheck pings Redis.
func RedisCheck(cl *redis.Client) health.Check {
	return health.Check{Name: "redis", Probe: func(ctx context.Context) error {
		return cl.Ping(ctx).Err()
	}}
}

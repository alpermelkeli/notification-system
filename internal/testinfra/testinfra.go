// Package testinfra starts the real dependencies (PostgreSQL, Kafka, Redis) in containers for
// integration tests. It is imported only from _test.go files; tests that use it skip under -short
// and need Docker otherwise.
//
// The images match docker-compose.yml, so tests run against the versions the stack runs.
package testinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Images, kept in sync with docker-compose.yml.
const (
	PostgresImage = "postgres:17-alpine"
	KafkaImage    = "apache/kafka:4.1.1"
	RedisImage    = "redis:7-alpine"
)

// Postgres starts PostgreSQL and returns a pool and a function that stops everything. The schema
// is not migrated: callers run storage.Migrate (this package cannot import storage, whose own
// tests use it).
func Postgres(ctx context.Context) (*pgxpool.Pool, func(), error) {
	ctr, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("notifier"),
		tcpostgres.WithUsername("notifier"),
		tcpostgres.WithPassword("notifier"),
		tcpostgres.BasicWaitStrategies(),
	)
	stop := func() { terminate(ctr) }
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("start postgres (integration tests need Docker; use -short to skip): %w", err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("postgres connection string: %w", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return pool, func() { pool.Close(); stop() }, nil
}

// Redis starts Redis and returns its host:port address and a function that stops it.
func Redis(ctx context.Context) (string, func(), error) {
	ctr, err := testcontainers.Run(ctx, RedisImage,
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("6379/tcp")),
	)
	stop := func() { terminate(ctr) }
	if err != nil {
		stop()
		return "", nil, fmt.Errorf("start redis (integration tests need Docker; use -short to skip): %w", err)
	}
	addr, err := ctr.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		stop()
		return "", nil, fmt.Errorf("redis endpoint: %w", err)
	}
	return addr, stop, nil
}

const kafkaStarter = "/tmp/testinfra-start.sh"

// Kafka starts a single-node KRaft broker and returns its bootstrap address and a function that
// stops it. Topics are not created; tests create the ones they need.
//
// The broker must advertise the host port Docker maps, which is known only after the container
// starts, so the container waits for a start script that a post-start hook writes.
func Kafka(ctx context.Context) (string, func(), error) {
	ctr, err := testcontainers.Run(ctx, KafkaImage,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
		}),
		testcontainers.WithEntrypoint("sh"),
		testcontainers.WithCmd("-c", "while [ ! -f "+kafkaStarter+" ]; do sleep 0.1; done; exec bash "+kafkaStarter),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{func(ctx context.Context, c testcontainers.Container) error {
				endpoint, err := c.PortEndpoint(ctx, "9092/tcp", "")
				if err != nil {
					return fmt.Errorf("kafka endpoint: %w", err)
				}
				script := "export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + endpoint + ",BROKER://localhost:29092\n" +
					"exec /etc/kafka/docker/run\n"
				if err := c.CopyToContainer(ctx, []byte(script), kafkaStarter, 0o755); err != nil {
					return fmt.Errorf("copy kafka start script: %w", err)
				}
				return wait.ForLog("Kafka Server started").WaitUntilReady(ctx, c)
			}},
		}),
	)
	stop := func() { terminate(ctr) }
	if err != nil {
		stop()
		return "", nil, fmt.Errorf("start kafka (integration tests need Docker; use -short to skip): %w", err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, "9092/tcp", "")
	if err != nil {
		stop()
		return "", nil, fmt.Errorf("kafka endpoint: %w", err)
	}
	return endpoint, stop, nil
}

func terminate(c testcontainers.Container) {
	if err := testcontainers.TerminateContainer(c); err != nil {
		slog.Warn("terminate test container", "err", err)
	}
}

// Package config loads the service configuration from environment variables.
//
// Configuration is parsed once at start-up into a typed struct. Every variable is documented in
// README.md and .env.example.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the configuration shared by every role of the notifier binary.
type Config struct {
	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// LogFormat is json (default) or text.
	LogFormat string
	// InstanceID identifies this process in logs, metrics and the X-Instance-ID header.
	InstanceID string
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration

	// HTTPAddr is the listen address of the public API (role: api).
	HTTPAddr string
	// AdminAddr is the listen address of the health/metrics server (roles: dispatcher, worker).
	AdminAddr string

	// DatabaseURL is the PostgreSQL connection string.
	DatabaseURL string
	// KafkaBrokers is the list of Kafka seed brokers.
	KafkaBrokers []string
	// RedisAddr is the Redis host:port.
	RedisAddr string
	// RedisPassword is optional.
	RedisPassword string

	// IdempotencyTTL is how long an Idempotency-Key is remembered (ADR-0004).
	IdempotencyTTL time.Duration
	// RetentionDays is how many days of notifications are kept before their daily partitions are
	// dropped (ADR-0008).
	RetentionDays int
	// PartitionPremakeDays is how many daily partitions are kept ready ahead of today.
	PartitionPremakeDays int
	// MaintenanceInterval is how often the dispatcher runs partition and key maintenance.
	MaintenanceInterval time.Duration
}

// Load reads the configuration from the process environment.
func Load() (Config, error) {
	return load(os.LookupEnv)
}

// lookupFunc matches os.LookupEnv so tests can inject an environment.
type lookupFunc func(key string) (string, bool)

func load(lookup lookupFunc) (Config, error) {
	p := parser{lookup: lookup}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "notifier"
	}

	cfg := Config{
		LogLevel:        p.oneOf("LOG_LEVEL", "info", "debug", "info", "warn", "error"),
		LogFormat:       p.oneOf("LOG_FORMAT", "json", "json", "text"),
		InstanceID:      p.str("INSTANCE_ID", hostname),
		ShutdownTimeout: p.duration("SHUTDOWN_TIMEOUT", 15*time.Second),

		HTTPAddr:  p.str("HTTP_ADDR", ":8080"),
		AdminAddr: p.str("ADMIN_ADDR", ":9090"),

		DatabaseURL:   p.str("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"),
		KafkaBrokers:  p.list("KAFKA_BROKERS", []string{"localhost:9092"}),
		RedisAddr:     p.str("REDIS_ADDR", "localhost:6379"),
		RedisPassword: p.str("REDIS_PASSWORD", ""),

		IdempotencyTTL:       p.duration("IDEMPOTENCY_TTL", 72*time.Hour),
		RetentionDays:        p.integer("RETENTION_DAYS", 30, 1, 3650),
		PartitionPremakeDays: p.integer("PARTITION_PREMAKE_DAYS", 7, 1, 90),
		MaintenanceInterval:  p.duration("MAINTENANCE_INTERVAL", time.Hour),
	}

	// A live idempotency key must never point at a notification whose partition was dropped.
	if cfg.IdempotencyTTL > time.Duration(cfg.RetentionDays)*24*time.Hour {
		p.errs = append(p.errs, fmt.Errorf("IDEMPOTENCY_TTL (%v) must not exceed RETENTION_DAYS (%d days)",
			cfg.IdempotencyTTL, cfg.RetentionDays))
	}

	if err := errors.Join(p.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// parser accumulates errors so that every invalid variable is reported at once.
type parser struct {
	lookup lookupFunc
	errs   []error
}

func (p *parser) raw(key string) (string, bool) {
	v, ok := p.lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	return v, v != ""
}

func (p *parser) str(key, def string) string {
	if v, ok := p.raw(key); ok {
		return v
	}
	return def
}

func (p *parser) oneOf(key, def string, allowed ...string) string {
	v := strings.ToLower(p.str(key, def))
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	p.errs = append(p.errs, fmt.Errorf("%s: %q is not one of %s", key, v, strings.Join(allowed, ", ")))
	return def
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a positive duration", key, v))
		return def
	}
	return d
}

func (p *parser) integer(key string, def, minV, maxV int) int {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < minV || n > maxV {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not an integer in [%d, %d]", key, v, minV, maxV))
		return def
	}
	return n
}

func (p *parser) list(key string, def []string) []string {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		p.errs = append(p.errs, fmt.Errorf("%s: must contain at least one value", key))
		return def
	}
	return out
}

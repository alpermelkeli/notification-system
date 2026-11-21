// Package config loads the service configuration from environment variables.
//
// Configuration is parsed once at start-up into a typed struct. Every variable is documented in
// README.md and .env.example.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
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
	// MaxScheduleAhead bounds how far in the future a notification may be scheduled (B-2).
	MaxScheduleAhead time.Duration
	// RequestTimeout bounds each public API request.
	RequestTimeout time.Duration

	// Retry is the delivery retry policy (ADR-0005): the worker's backoff, and the attempt budget
	// that both the worker and the dispatcher's reaper enforce.
	Retry Retry
	// Dispatcher configures the relay and reaper (role: dispatcher).
	Dispatcher Dispatcher
	// Worker configures delivery (role: worker).
	Worker Worker
}

// Retry configures exponential backoff with equal jitter (ADR-0005, FR-3.1).
type Retry struct {
	// BaseDelay is the backoff ceiling of the first retry; it doubles per attempt.
	BaseDelay time.Duration
	// MaxDelay caps the backoff ceiling.
	MaxDelay time.Duration
	// MaxAttempts is the attempt budget of a notification.
	MaxAttempts int
}

// Dispatcher configures the dispatcher role (ADR-0003).
type Dispatcher struct {
	// BatchSize is the number of due rows published per transaction.
	BatchSize int
	// PollInterval is the fallback poll period for scheduled and retrying rows.
	PollInterval time.Duration
	// ReaperInterval is how often expired leases and lost queued rows are recovered.
	ReaperInterval time.Duration
	// ReaperGrace is how long a queued row behind the committed offset is left alone.
	ReaperGrace time.Duration
	// ReaperStuckAfter is the age fallback for queued rows.
	ReaperStuckAfter time.Duration
}

// Worker configures the worker role (ADR-0002, ADR-0005, ADR-0006).
type Worker struct {
	// Channels are the channels this process delivers; each gets its own consumer group member.
	Channels []domain.Channel
	// Concurrency is the number of sends in flight per channel.
	Concurrency int
	// ProviderURL is the delivery provider endpoint (webhook.site or the mock provider).
	ProviderURL string
	// ProviderTimeout bounds each provider request.
	ProviderTimeout time.Duration
	// Lease is how long a claim lasts; it must cover the provider timeout.
	Lease time.Duration
	// RateLimits is the per-channel send rate per second, shared by all workers.
	RateLimits map[domain.Channel]int
	// RateLimitBurst is the burst allowance of each channel's bucket.
	RateLimitBurst int
	// AgingRate is the scheduler's aging rate in points per second waited.
	AgingRate float64
	// MinShareEvery reserves one pick in this many for non-high traffic (0 disables).
	MinShareEvery int
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
		MaxScheduleAhead:     p.duration("MAX_SCHEDULE_AHEAD", 7*24*time.Hour),
		RequestTimeout:       p.duration("REQUEST_TIMEOUT", 10*time.Second),

		Retry: Retry{
			BaseDelay:   p.duration("RETRY_BASE_DELAY", 5*time.Second),
			MaxDelay:    p.duration("RETRY_MAX_DELAY", 5*time.Minute),
			MaxAttempts: p.integer("MAX_ATTEMPTS", 5, 1, 100),
		},
		Dispatcher: Dispatcher{
			BatchSize:        p.integer("DISPATCH_BATCH_SIZE", 500, 1, 10_000),
			PollInterval:     p.duration("DISPATCH_POLL_INTERVAL", 500*time.Millisecond),
			ReaperInterval:   p.duration("REAPER_INTERVAL", 30*time.Second),
			ReaperGrace:      p.duration("REAPER_GRACE", time.Minute),
			ReaperStuckAfter: p.duration("REAPER_STUCK_AFTER", 24*time.Hour),
		},
		Worker: Worker{
			Channels:        p.channels("WORKER_CHANNELS"),
			Concurrency:     p.integer("WORKER_CONCURRENCY", 20, 1, 1000),
			ProviderURL:     p.url("PROVIDER_URL", "http://localhost:8081/"),
			ProviderTimeout: p.duration("PROVIDER_TIMEOUT", 10*time.Second),
			Lease:           p.duration("LEASE_DURATION", 30*time.Second),
			RateLimits: map[domain.Channel]int{
				domain.ChannelSMS:   p.integer("RATE_LIMIT_SMS", 100, 1, 100_000),
				domain.ChannelEmail: p.integer("RATE_LIMIT_EMAIL", 100, 1, 100_000),
				domain.ChannelPush:  p.integer("RATE_LIMIT_PUSH", 100, 1, 100_000),
			},
			RateLimitBurst: p.integer("RATE_LIMIT_BURST", 1, 1, 10_000),
			AgingRate:      p.float("SCHEDULER_AGING_RATE", 2, 0, 1000),
			MinShareEvery:  p.integer("SCHEDULER_MIN_SHARE", 10, 0, 1000),
		},
	}

	// A claim must outlive the provider call and the outcome write, or a slow but successful
	// send is reaped and delivered twice.
	if cfg.Worker.Lease < cfg.Worker.ProviderTimeout+5*time.Second {
		p.errs = append(p.errs, fmt.Errorf("LEASE_DURATION (%v) must be at least PROVIDER_TIMEOUT (%v) + 5s",
			cfg.Worker.Lease, cfg.Worker.ProviderTimeout))
	}

	if cfg.Retry.MaxDelay < cfg.Retry.BaseDelay {
		p.errs = append(p.errs, fmt.Errorf("RETRY_MAX_DELAY (%v) must not be shorter than RETRY_BASE_DELAY (%v)",
			cfg.Retry.MaxDelay, cfg.Retry.BaseDelay))
	}

	// A live idempotency key must never point at a notification whose partition was dropped.
	if cfg.IdempotencyTTL > time.Duration(cfg.RetentionDays)*24*time.Hour {
		p.errs = append(p.errs, fmt.Errorf("IDEMPOTENCY_TTL (%v) must not exceed RETENTION_DAYS (%d days)",
			cfg.IdempotencyTTL, cfg.RetentionDays))
	}

	// A scheduled notification must be delivered, retries included, before its partition is
	// dropped, so keep at least a day of margin.
	if cfg.MaxScheduleAhead > time.Duration(cfg.RetentionDays-1)*24*time.Hour {
		p.errs = append(p.errs, fmt.Errorf("MAX_SCHEDULE_AHEAD (%v) must be at least one day shorter than RETENTION_DAYS (%d days)",
			cfg.MaxScheduleAhead, cfg.RetentionDays))
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

func (p *parser) float(key string, def, minV, maxV float64) float64 {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < minV || f > maxV {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a number in [%g, %g]", key, v, minV, maxV))
		return def
	}
	return f
}

func (p *parser) url(key, def string) string {
	v := p.str(key, def)
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not an absolute http(s) URL", key, v))
		return def
	}
	return v
}

// channels parses a channel list; the default is every channel.
func (p *parser) channels(key string) []domain.Channel {
	names := p.list(key, nil)
	if names == nil {
		return slices.Clone(domain.Channels)
	}
	var out []domain.Channel
	for _, n := range names {
		c, err := domain.ParseChannel(strings.ToLower(n))
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("%s: %w", key, err))
			continue
		}
		if !slices.Contains(out, c) {
			out = append(out, c)
		}
	}
	return out
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

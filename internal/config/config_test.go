package config

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func env(m map[string]string) lookupFunc {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.LogLevel != "info" || cfg.LogFormat != "json" {
		t.Errorf("log defaults = %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
	if cfg.HTTPAddr != ":8080" || cfg.AdminAddr != ":9090" {
		t.Errorf("addr defaults = %q/%q", cfg.HTTPAddr, cfg.AdminAddr)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %v", cfg.ShutdownTimeout)
	}
	if cfg.InstanceID == "" {
		t.Error("InstanceID should default to the hostname")
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:9092"}) {
		t.Errorf("KafkaBrokers = %v", cfg.KafkaBrokers)
	}
	if cfg.IdempotencyTTL != 72*time.Hour || cfg.RetentionDays != 30 ||
		cfg.PartitionPremakeDays != 7 || cfg.MaintenanceInterval != time.Hour {
		t.Errorf("lifecycle defaults = %v/%d/%d/%v",
			cfg.IdempotencyTTL, cfg.RetentionDays, cfg.PartitionPremakeDays, cfg.MaintenanceInterval)
	}
}

func TestLoadEngineDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Dispatcher != (Dispatcher{
		BatchSize: 500, PollInterval: 500 * time.Millisecond, ReaperInterval: 30 * time.Second,
		ReaperGrace: time.Minute, ReaperStuckAfter: 24 * time.Hour,
	}) {
		t.Errorf("dispatcher defaults = %+v", cfg.Dispatcher)
	}
	w := cfg.Worker
	if !reflect.DeepEqual(w.Channels, []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}) ||
		w.Concurrency != 20 || w.ProviderURL != "http://localhost:8081/" || w.ProviderTimeout != 10*time.Second ||
		w.Lease != 30*time.Second || w.RateLimitBurst != 10 || w.RetryDelay != 5*time.Second ||
		w.MaxAttempts != 5 || w.AgingRate != 2 || w.MinShareEvery != 10 {
		t.Errorf("worker defaults = %+v", w)
	}
	for _, c := range domain.Channels {
		if w.RateLimits[c] != 100 {
			t.Errorf("rate limit of %s = %d, want 100", c, w.RateLimits[c])
		}
	}
}

func TestLoadEngineOverrides(t *testing.T) {
	cfg, err := load(env(map[string]string{
		"WORKER_CHANNELS":      "SMS, push,sms",
		"PROVIDER_URL":         "https://webhook.site/abc",
		"RATE_LIMIT_EMAIL":     "600",
		"SCHEDULER_AGING_RATE": "0.5",
		"SCHEDULER_MIN_SHARE":  "0",
		"REAPER_GRACE":         "2m",
	}))
	if err != nil {
		t.Fatal(err)
	}
	w := cfg.Worker
	if !reflect.DeepEqual(w.Channels, []domain.Channel{domain.ChannelSMS, domain.ChannelPush}) ||
		w.ProviderURL != "https://webhook.site/abc" || w.RateLimits[domain.ChannelEmail] != 600 ||
		w.AgingRate != 0.5 || w.MinShareEvery != 0 || cfg.Dispatcher.ReaperGrace != 2*time.Minute {
		t.Errorf("worker = %+v, dispatcher = %+v", w, cfg.Dispatcher)
	}
}

func TestLoadRejectsInvalidEngineSettings(t *testing.T) {
	_, err := load(env(map[string]string{
		"WORKER_CHANNELS":      "sms,fax",
		"PROVIDER_URL":         "webhook.site/abc",
		"SCHEDULER_AGING_RATE": "fast",
		"RATE_LIMIT_SMS":       "0",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{"WORKER_CHANNELS", "PROVIDER_URL", "SCHEDULER_AGING_RATE", "RATE_LIMIT_SMS"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
	if _, err := load(env(map[string]string{"PROVIDER_TIMEOUT": "30s", "LEASE_DURATION": "30s"})); err == nil ||
		!strings.Contains(err.Error(), "LEASE_DURATION") {
		t.Errorf("a lease shorter than the provider timeout + 5s must be rejected: %v", err)
	}
}

func TestLoadOverrides(t *testing.T) {
	defaults, err := load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := load(env(map[string]string{
		"LOG_LEVEL":          "DEBUG",
		"LOG_FORMAT":         "text",
		"INSTANCE_ID":        "api-1",
		"SHUTDOWN_TIMEOUT":   "3s",
		"HTTP_ADDR":          ":18080",
		"KAFKA_BROKERS":      " k1:9092, ,k2:9092 ",
		"REDIS_ADDR":         "redis:6379",
		"DATABASE_URL":       "postgres://x",
		"IDEMPOTENCY_TTL":    "24h",
		"RETENTION_DAYS":     "7",
		"MAX_SCHEDULE_AHEAD": "72h",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := Config{
		LogLevel:        "debug",
		LogFormat:       "text",
		InstanceID:      "api-1",
		ShutdownTimeout: 3 * time.Second,
		HTTPAddr:        ":18080",
		AdminAddr:       ":9090",
		DatabaseURL:     "postgres://x",
		KafkaBrokers:    []string{"k1:9092", "k2:9092"},
		RedisAddr:       "redis:6379",

		IdempotencyTTL:       24 * time.Hour,
		RetentionDays:        7,
		PartitionPremakeDays: 7,
		MaintenanceInterval:  time.Hour,
		MaxScheduleAhead:     72 * time.Hour,
		RequestTimeout:       10 * time.Second,

		Dispatcher: defaults.Dispatcher,
		Worker:     defaults.Worker,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("got  %+v\nwant %+v", cfg, want)
	}
}

func TestLoadBlankValueUsesDefault(t *testing.T) {
	cfg, err := load(env(map[string]string{"HTTP_ADDR": "   "}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want default", cfg.HTTPAddr)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(env(map[string]string{
		"LOG_LEVEL":              "verbose",
		"LOG_FORMAT":             "xml",
		"SHUTDOWN_TIMEOUT":       "-1s",
		"KAFKA_BROKERS":          " , ",
		"RETENTION_DAYS":         "0",
		"PARTITION_PREMAKE_DAYS": "many",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{
		"LOG_LEVEL", "LOG_FORMAT", "SHUTDOWN_TIMEOUT", "KAFKA_BROKERS", "RETENTION_DAYS", "PARTITION_PREMAKE_DAYS",
	} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}

func TestLoadRejectsTTLLongerThanRetention(t *testing.T) {
	_, err := load(env(map[string]string{"IDEMPOTENCY_TTL": "72h", "RETENTION_DAYS": "2"}))
	if err == nil || !strings.Contains(err.Error(), "IDEMPOTENCY_TTL") {
		t.Fatalf("err = %v, want an IDEMPOTENCY_TTL vs RETENTION_DAYS error", err)
	}
}

func TestLoadRejectsScheduleBeyondRetention(t *testing.T) {
	_, err := load(env(map[string]string{"MAX_SCHEDULE_AHEAD": "240h", "RETENTION_DAYS": "10", "IDEMPOTENCY_TTL": "24h"}))
	if err == nil || !strings.Contains(err.Error(), "MAX_SCHEDULE_AHEAD") {
		t.Fatalf("err = %v, want a MAX_SCHEDULE_AHEAD vs RETENTION_DAYS error", err)
	}
}

func TestLoadRequiresADayOfMarginForSchedules(t *testing.T) {
	if _, err := load(env(map[string]string{"MAX_SCHEDULE_AHEAD": "216h", "RETENTION_DAYS": "10", "IDEMPOTENCY_TTL": "24h"})); err != nil {
		t.Errorf("9 days ahead with 10 days retention should be accepted: %v", err)
	}
	if _, err := load(env(map[string]string{"MAX_SCHEDULE_AHEAD": "217h", "RETENTION_DAYS": "10", "IDEMPOTENCY_TTL": "24h"})); err == nil {
		t.Error("less than a day of margin should be rejected")
	}
}

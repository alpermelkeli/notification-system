package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
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

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(env(map[string]string{
		"LOG_LEVEL":        "DEBUG",
		"LOG_FORMAT":       "text",
		"INSTANCE_ID":      "api-1",
		"SHUTDOWN_TIMEOUT": "3s",
		"HTTP_ADDR":        ":18080",
		"KAFKA_BROKERS":    " k1:9092, ,k2:9092 ",
		"REDIS_ADDR":       "redis:6379",
		"DATABASE_URL":     "postgres://x",
		"IDEMPOTENCY_TTL":  "24h",
		"RETENTION_DAYS":   "7",
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

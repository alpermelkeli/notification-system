// Command mockprovider runs a local fake of the external delivery provider for docker-compose,
// demos and load tests. See internal/mockprovider.
//
// Environment:
//
//	MOCK_ADDR              listen address (default :8081)
//	MOCK_LATENCY           base latency (default 50ms)
//	MOCK_LATENCY_JITTER    random extra latency in [0, jitter) (default 100ms)
//	MOCK_FAILURE_RATE      probability of a 500 response (default 0)
//	MOCK_RATE_LIMIT_RATE   probability of a 429 response (default 0)
//	MOCK_PERMANENT_RATE    probability of a 400 response, which is not retried (default 0)
//	MOCK_RETRY_AFTER       Retry-After sent with 429 (default 1s)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alpermelkeli/notification-system/internal/mockprovider"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockprovider")
	if err := run(logger); err != nil {
		logger.Error("stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var errs []error
	dur := func(key string, def time.Duration) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			errs = append(errs, fmt.Errorf("%s: %q is not a duration", key, v))
		}
		return d
	}
	prob := func(key string) float64 {
		v := os.Getenv(key)
		if v == "" {
			return 0
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || math.IsNaN(f) || f < 0 || f > 1 {
			errs = append(errs, fmt.Errorf("%s: %q is not a probability in [0,1]", key, v))
		}
		return f
	}

	cfg := mockprovider.Config{
		Latency:              dur("MOCK_LATENCY", 50*time.Millisecond),
		Jitter:               dur("MOCK_LATENCY_JITTER", 100*time.Millisecond),
		FailureRate:          prob("MOCK_FAILURE_RATE"),
		RateLimitRate:        prob("MOCK_RATE_LIMIT_RATE"),
		PermanentFailureRate: prob("MOCK_PERMANENT_RATE"),
		RetryAfter:           dur("MOCK_RETRY_AFTER", time.Second),
	}
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/", mockprovider.New(cfg, reg).Handler())

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }() // a second signal kills the process

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	logger.Info("mock provider listening", "addr", addr, "config", fmt.Sprintf("%+v", cfg))

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		return err
	}
	return nil
}

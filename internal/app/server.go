// Package app wires configuration, dependencies and HTTP servers into the runnable roles of
// the notifier binary: api, dispatcher and worker.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpermelkeli/notification-system/internal/health"
	"github.com/alpermelkeli/notification-system/internal/httpx"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/playground"
	"github.com/alpermelkeli/notification-system/internal/telemetry"
)

const readinessTimeout = 2 * time.Second

// Probe and scrape endpoints, served by every role.
const (
	pathLiveness  = "/healthz"
	pathReadiness = "/readyz"
	pathMetrics   = "/metrics"
	// pathWebSocket is the API's status stream (B-4). Its connections last for hours, so it gets
	// no request span or HTTP metrics; the stream has metrics of its own.
	pathWebSocket = "/v1/ws"
)

// baseRouter returns a router with the shared middleware chain and the health and metrics
// endpoints mounted. reg is the role's metrics registry, served on /metrics; HTTP request
// metrics are registered on it too and returned for the API's own counters.
func baseRouter(logger *slog.Logger, instance string, checks []health.Check, reg *prometheus.Registry) (chi.Router, *metrics.HTTP) {
	m := metrics.NewHTTP(reg)
	r := chi.NewRouter()
	// Order matters: the correlation ID must exist before anything logs or traces, the span
	// before the request log (so the log line carries the trace id), and the recoverer sits
	// inside the request logger so a recovered panic is logged as a 500. The playground's stats
	// endpoint is polled every 2 s by each open page; like the probes, it gets no span, request
	// log or HTTP metrics, so it does not drown the real traffic.
	r.Use(
		httpx.CorrelationID,
		httpx.InstanceID(instance),
		telemetry.HTTPMiddleware(pathLiveness, pathReadiness, pathMetrics, pathWebSocket, playground.PathStats),
		m.Middleware(pathWebSocket, playground.PathStats),
		httpx.RequestLogger(logger, pathLiveness, pathReadiness, pathMetrics, playground.PathStats),
		httpx.Recoverer(logger),
	)
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "resource not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})

	r.Get(pathLiveness, health.Liveness())
	r.Get(pathReadiness, health.Readiness(logger, readinessTimeout, checks...))
	r.Method(http.MethodGet, pathMetrics, metrics.Handler(reg))
	return r, m
}

// serveHTTP runs an HTTP server until ctx is cancelled, then shuts it down gracefully within
// shutdownTimeout.
func serveHTTP(ctx context.Context, logger *slog.Logger, addr string, h http.Handler, shutdownTimeout time.Duration) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	return serveListener(ctx, logger, ln, h, shutdownTimeout)
}

func serveListener(ctx context.Context, logger *slog.Logger, ln net.Listener, h http.Handler, shutdownTimeout time.Duration) error {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
		// No WriteTimeout: it would cut long-lived WebSocket connections (B-4). Handlers bound
		// their own work with context deadlines instead.
		// No BaseContext tied to ctx either: cancelling ctx starts the shutdown, and in-flight
		// requests must be allowed to finish rather than be cancelled with it.
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", ln.Addr().String())
		errCh <- srv.Serve(ln)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("http server shutting down", "timeout", shutdownTimeout.String())
	// The parent context is already cancelled; shutdown gets its own deadline.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Deadline hit with requests still running: drop the remaining connections.
		_ = srv.Close()
		return fmt.Errorf("http server shutdown: %w", err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}

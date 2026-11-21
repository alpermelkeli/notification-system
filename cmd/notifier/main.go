// Command notifier is the single binary of the notification system. The first argument selects
// the role (ADR-0007):
//
//	notifier api           public HTTP API
//	notifier dispatcher    PostgreSQL -> Kafka relay and reaper
//	notifier worker        channel consumers and provider delivery
//	notifier healthcheck   probe a local health endpoint (for container health checks)
//	notifier version       print the build version
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alpermelkeli/notification-system/internal/app"
	"github.com/alpermelkeli/notification-system/internal/config"
	"github.com/alpermelkeli/notification-system/internal/logging"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

type roleFunc func(context.Context, config.Config, *slog.Logger) error

var roles = map[string]roleFunc{
	"api":        app.RunAPI,
	"dispatcher": app.RunDispatcher,
	"worker":     app.RunWorker,
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}

	switch args[0] {
	case "version", "--version":
		_, _ = fmt.Fprintln(stdout, version)
		return 0
	case "healthcheck":
		return healthcheck(args[1:], stderr)
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	}

	role, ok := roles[args[0]]
	if !ok {
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	logger := logging.New(stdout, logging.Options{
		Level: cfg.LogLevel, Format: cfg.LogFormat, Service: args[0], Instance: cfg.InstanceID,
	})
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		// After the first signal, restore default handling so a second one kills the process.
		<-ctx.Done()
		stop()
	}()

	logger.Info("starting", "version", version)
	if err := role(ctx, cfg, logger); err != nil {
		logger.Error("stopped with error", "err", err)
		return 1
	}
	logger.Info("stopped")
	return 0
}

// healthcheck exits 0 if the URL (default: the local API liveness endpoint) answers 200. The
// runtime image has no shell or curl, so docker-compose health checks call the binary itself.
func healthcheck(args []string, stderr io.Writer) int {
	url := "http://127.0.0.1:8080/healthz"
	if len(args) > 0 {
		url = args[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The URL is an operator-supplied argument of a local health probe, not user input.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // G704, see above
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704, see above
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = fmt.Fprintf(stderr, "%s returned %d\n", url, resp.StatusCode)
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	_, _ = fmt.Fprint(w, `usage: notifier <command>

commands:
  api           serve the public HTTP API
  dispatcher    relay due notifications from PostgreSQL to Kafka
  worker        consume channel topics and deliver notifications
  healthcheck   [url] exit 0 if url (default http://127.0.0.1:8080/healthz) returns 200
  version       print the build version

Configuration is read from environment variables; see .env.example.
`)
}

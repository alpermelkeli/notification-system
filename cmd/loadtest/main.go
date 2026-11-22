// Command loadtest drives a running stack (`make up` or `make up-scaled`) and checks it against
// docs/capacity-planning.md: how fast the API accepts a burst, that no channel goes over its
// rate limit while the burst drains, how fast it drains, and how long a high-priority
// notification takes with and without a backlog in front of it.
//
// It talks only to the public API and to the mock provider's /stats and /admin/faults. It refuses
// to start unless the mock provider answers with no faults and no earlier notifications are still
// in flight, and it sends the burst only after the mock provider has received the first probes,
// so a stack whose PROVIDER_URL points at webhook.site gets a few probes, never the burst.
//
// Usage:
//
//	go run ./cmd/loadtest                 # or: make loadtest
//	go run ./cmd/loadtest -total 90000 -concurrency 8
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	apiURL      string
	providerURL string
	total       int
	batchSize   int
	concurrency int
	probes      int
	limit       int
	timeout     time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.apiURL, "api", "http://localhost:8080", "base URL of the API (through the load balancer)")
	flag.StringVar(&cfg.providerURL, "provider", "http://localhost:8081", "base URL of the mock provider")
	flag.IntVar(&cfg.total, "total", 30000, "notifications in the burst, spread evenly over sms, email and push")
	flag.IntVar(&cfg.batchSize, "batch", 1000, "notifications per batch request (1 to 1000)")
	flag.IntVar(&cfg.concurrency, "concurrency", 4, "batch requests in flight at once")
	flag.IntVar(&cfg.probes, "probes", 30, "high-priority notifications sent for each latency measurement")
	flag.IntVar(&cfg.limit, "limit", 100, "configured rate limit per channel (RATE_LIMIT_*), in messages per second")
	flag.DurationVar(&cfg.timeout, "timeout", 20*time.Minute, "give up if the run takes longer than this")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	ok, err := run(ctx, cfg, os.Stdout)
	cancel()
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

func (c config) validate() error {
	switch {
	case c.batchSize < 1 || c.batchSize > 1000:
		return errors.New("-batch must be from 1 to 1000")
	case c.total < 3*c.batchSize:
		return errors.New("-total must be at least three batches, so every channel gets a backlog")
	case c.concurrency < 1:
		return errors.New("-concurrency must be at least 1")
	case c.probes < 1:
		return errors.New("-probes must be at least 1")
	case c.limit < 1:
		return errors.New("-limit must be at least 1")
	}
	return nil
}

// run executes the phases in order and prints the report. It returns false if a check failed.
func run(ctx context.Context, cfg config, out io.Writer) (bool, error) {
	c := newClient(cfg.apiURL, cfg.providerURL)

	if err := c.preflight(ctx); err != nil {
		return false, err
	}
	fmt.Fprintf(out, "Load test: %d notifications in batches of %d, %d requests in flight, limit %d/s per channel\n\n",
		cfg.total, cfg.batchSize, cfg.concurrency, cfg.limit)

	fmt.Fprintf(out, "1/4 High priority, no backlog: %d notifications, 10 per second\n", cfg.probes)
	start, err := c.providerStats(ctx)
	if err != nil {
		return false, err
	}
	idle, err := c.probe(ctx, cfg.probes, 100*time.Millisecond, channels)
	if err != nil {
		return false, fmt.Errorf("probe without backlog: %w", err)
	}
	// Only the burst is large; check with the probes that the workers deliver to this mock
	// provider and not to a PROVIDER_URL set elsewhere (webhook.site).
	before, err := c.providerStats(ctx)
	if err != nil {
		return false, err
	}
	if got := before.acceptedSince(start); got < int64(len(idle.latencies)) {
		return false, fmt.Errorf("the mock provider accepted %d of the %d probes; the workers deliver somewhere else (check PROVIDER_URL), so the burst is not sent",
			got, len(idle.latencies))
	}

	// The rate watch starts before the burst: workers send as soon as its first batch is
	// dispatched.
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	watched := make(chan rateWatch, 1)
	go func() { watched <- c.watchRates(watchCtx, 500*time.Millisecond) }()

	fmt.Fprintf(out, "2/4 Burst: sending %d low-priority notifications\n", cfg.total)
	burst, err := c.burst(ctx, cfg)
	if err != nil {
		return false, fmt.Errorf("burst: %w", err)
	}

	// The probe runs while the burst drains.
	fmt.Fprintf(out, "3/4 High priority behind the backlog: %d SMS, 2 per second\n", cfg.probes)

	busy, err := c.probe(ctx, cfg.probes, 500*time.Millisecond, []string{"sms"})
	if err != nil {
		return false, fmt.Errorf("probe behind the backlog: %w", err)
	}

	fmt.Fprintln(out, "4/4 Waiting for the burst to drain")
	drained, err := c.waitDrained(ctx, burst.batchIDs, cfg.total, out)
	stopWatch()
	rates := <-watched
	if err != nil {
		return false, fmt.Errorf("drain: %w", err)
	}
	got, err := c.deliveries(ctx, burst.batchIDs)
	if err != nil {
		return false, fmt.Errorf("reading delivery times: %w", err)
	}
	after, err := c.providerStats(ctx)
	if err != nil {
		return false, err
	}

	r := report{
		cfg:      cfg,
		idle:     idle,
		busy:     busy,
		burst:    burst,
		drained:  drained,
		rates:    rates,
		sent:     got.sent,
		queued:   got.lastQueued,
		rejected: after.rejectedSince(before),
	}
	return r.print(out), nil
}

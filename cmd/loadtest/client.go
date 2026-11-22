package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var channels = []string{"sms", "email", "push"}

// client calls the API and the mock provider.
type client struct {
	api      string
	provider string
	http     *http.Client
}

func newClient(apiURL, providerURL string) *client {
	return &client{
		api:      strings.TrimRight(apiURL, "/"),
		provider: strings.TrimRight(providerURL, "/"),
		http:     &http.Client{Timeout: 30 * time.Second},
	}
}

type item struct {
	Recipient string `json:"recipient"`
	Channel   string `json:"channel"`
	Content   string `json:"content"`
	Priority  string `json:"priority"`
}

func newItem(channel string, n int, priority string) item {
	it := item{Channel: channel, Priority: priority, Content: fmt.Sprintf("Load test message %d", n)}
	switch channel {
	case "sms":
		it.Recipient = fmt.Sprintf("+90555%07d", n%10_000_000)
	case "email":
		it.Recipient = fmt.Sprintf("load%d@example.com", n)
	default:
		it.Recipient = fmt.Sprintf("fcm-token-%08d", n)
	}
	return it
}

type notification struct {
	ID        string     `json:"id"`
	Channel   string     `json:"channel"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"createdAt"`
	QueuedAt  *time.Time `json:"queuedAt"`
	SentAt    *time.Time `json:"sentAt"`
}

func (n notification) finished() bool {
	return n.Status == "sent" || n.Status == "failed" || n.Status == "cancelled"
}

// do sends a request and decodes a JSON answer with the expected status into dst.
func (c *client) do(ctx context.Context, method, rawURL string, body any, want int, dst any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req) //nolint:gosec // G704: the URLs come from the command line of whoever runs the test
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: got %d, want %d: %s", method, rawURL, resp.StatusCode, want, bytes.TrimSpace(msg))
	}
	if dst == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// preflight checks that the API is ready and that the provider is the mock, with no faults.
func (c *client) preflight(ctx context.Context) error {
	if err := c.do(ctx, http.MethodGet, c.api+"/readyz", nil, http.StatusOK, nil); err != nil {
		return fmt.Errorf("the API is not ready (is the stack up?): %w", err)
	}
	var f struct {
		FailureRate   float64 `json:"failureRate"`
		RateLimitRate float64 `json:"rateLimitRate"`
		PermanentRate float64 `json:"permanentRate"`
	}
	if err := c.do(ctx, http.MethodGet, c.provider+"/admin/faults", nil, http.StatusOK, &f); err != nil {
		return fmt.Errorf("the mock provider does not answer, and the test only runs against it: %w", err)
	}
	if f.FailureRate+f.RateLimitRate+f.PermanentRate > 0 {
		return fmt.Errorf("the mock provider is injecting faults (%+v); restore it first", f)
	}
	return nil
}

type probeResult struct {
	latencies []time.Duration // createdAt to sentAt, for the ones sent
	dispatch  []time.Duration // createdAt to queuedAt: waiting for the dispatcher
	worker    []time.Duration // queuedAt to sentAt: Kafka, the worker's scheduler, the provider
	failed    int
}

// probe sends n high-priority notifications, one per interval, cycling through chs, and waits
// until all of them finish.
func (c *client) probe(ctx context.Context, n int, interval time.Duration, chs []string) (probeResult, error) {
	pending := make(map[string]bool, n)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for i := range n {
		if i > 0 {
			select {
			case <-ctx.Done():
				return probeResult{}, ctx.Err()
			case <-tick.C:
			}
		}
		var created notification
		err := c.do(ctx, http.MethodPost, c.api+"/v1/notifications",
			newItem(chs[i%len(chs)], 9_000_000+i, "high"), http.StatusAccepted, &created)
		if err != nil {
			return probeResult{}, err
		}
		pending[created.ID] = true
	}

	var res probeResult
	for len(pending) > 0 {
		for id := range pending {
			var got notification
			if err := c.do(ctx, http.MethodGet, c.api+"/v1/notifications/"+id, nil, http.StatusOK, &got); err != nil {
				return probeResult{}, err
			}
			if !got.finished() {
				continue
			}
			delete(pending, id)
			if got.Status == "sent" && got.SentAt != nil {
				res.latencies = append(res.latencies, got.SentAt.Sub(got.CreatedAt))
				if got.QueuedAt != nil {
					res.dispatch = append(res.dispatch, got.QueuedAt.Sub(got.CreatedAt))
					res.worker = append(res.worker, got.SentAt.Sub(*got.QueuedAt))
				}
			} else {
				res.failed++
			}
		}
		if len(pending) > 0 {
			select {
			case <-ctx.Done():
				return probeResult{}, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return res, nil
}

type burstResult struct {
	start     time.Time
	elapsed   time.Duration   // first request sent to last answer received
	requests  []time.Duration // duration of each batch request
	batchIDs  []string
	accepted  int
	perSecond float64
}

// burst sends cfg.total low-priority notifications as batches, cfg.concurrency at a time. Each
// batch mixes the three channels.
func (c *client) burst(ctx context.Context, cfg config) (burstResult, error) {
	batches := (cfg.total + cfg.batchSize - 1) / cfg.batchSize
	jobs := make(chan int)
	type answer struct {
		id  string
		dur time.Duration
		n   int
		err error
	}
	answers := make(chan answer, batches)

	var wg sync.WaitGroup
	for range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range jobs {
				first := b * cfg.batchSize
				last := min(first+cfg.batchSize, cfg.total)
				items := make([]item, 0, last-first)
				for i := first; i < last; i++ {
					items = append(items, newItem(channels[i%len(channels)], i, "low"))
				}
				var resp struct {
					Batch struct {
						ID string `json:"id"`
					} `json:"batch"`
				}
				t := time.Now()
				err := c.do(ctx, http.MethodPost, c.api+"/v1/notifications/batch",
					map[string]any{"notifications": items}, http.StatusAccepted, &resp)
				answers <- answer{id: resp.Batch.ID, dur: time.Since(t), n: len(items), err: err}
			}
		}()
	}

	start := time.Now()
	go func() {
		defer close(jobs)
		for b := range batches {
			select {
			case <-ctx.Done():
				return
			case jobs <- b:
			}
		}
	}()
	wg.Wait()
	close(answers)

	res := burstResult{start: start, elapsed: time.Since(start)}
	var errs []error
	for a := range answers {
		if a.err != nil {
			errs = append(errs, a.err)
			continue
		}
		res.batchIDs = append(res.batchIDs, a.id)
		res.requests = append(res.requests, a.dur)
		res.accepted += a.n
	}
	if err := errors.Join(errs...); err != nil {
		return res, err
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	res.perSecond = float64(res.accepted) / res.elapsed.Seconds()
	return res, nil
}

type providerStats struct {
	Accepted       map[string]int64 `json:"accepted"`
	Rejected       map[string]int64 `json:"rejected"`
	RateLastSecond map[string]int64 `json:"rate_last_second"`
}

func (c *client) providerStats(ctx context.Context) (providerStats, error) {
	var s providerStats
	err := c.do(ctx, http.MethodGet, c.provider+"/stats", nil, http.StatusOK, &s)
	return s, err
}

// rejectedSince sums the simulated 429 and 500 answers given after before was taken.
func (s providerStats) rejectedSince(before providerStats) int64 {
	var n int64
	for ch, v := range s.Rejected {
		n += v - before.Rejected[ch]
	}
	return n
}

type rateWatch struct {
	max     map[string]int64 // highest requests per second the provider saw, per channel
	samples int
	errors  int
}

// watchRates samples the provider's per-second request counts until ctx is cancelled. Sampling
// twice a second sees every full second at least once.
func (c *client) watchRates(ctx context.Context, every time.Duration) rateWatch {
	w := rateWatch{max: make(map[string]int64)}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return w
		case <-tick.C:
		}
		s, err := c.providerStats(ctx)
		if err != nil {
			if ctx.Err() == nil {
				w.errors++
			}
			continue
		}
		w.samples++
		for ch, v := range s.RateLastSecond {
			w.max[ch] = max(w.max[ch], v)
		}
	}
}

type drainResult struct {
	at     time.Time
	counts map[string]int
}

// waitDrained polls the batches until every notification in them has finished.
func (c *client) waitDrained(ctx context.Context, batchIDs []string, total int, out io.Writer) (drainResult, error) {
	lastPrint := time.Now()
	for {
		counts := make(map[string]int)
		for _, id := range batchIDs {
			var b struct {
				StatusCounts map[string]int `json:"statusCounts"`
			}
			if err := c.do(ctx, http.MethodGet, c.api+"/v1/batches/"+id, nil, http.StatusOK, &b); err != nil {
				return drainResult{}, err
			}
			for st, n := range b.StatusCounts {
				counts[st] += n
			}
		}
		done := counts["sent"] + counts["failed"] + counts["cancelled"]
		if done >= total {
			return drainResult{at: time.Now(), counts: counts}, nil
		}
		if time.Since(lastPrint) >= 15*time.Second {
			fmt.Fprintf(out, "    %d of %d finished\n", done, total)
			lastPrint = time.Now()
		}
		select {
		case <-ctx.Done():
			return drainResult{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

type delivered struct {
	sent       map[string][]time.Time // sentAt per channel
	lastQueued time.Time              // the latest queuedAt: when the dispatcher had published all
}

// deliveries lists every notification of the batches.
func (c *client) deliveries(ctx context.Context, batchIDs []string) (delivered, error) {
	out := delivered{sent: make(map[string][]time.Time)}
	for _, id := range batchIDs {
		cursor := ""
		for {
			q := url.Values{"batchId": {id}, "limit": {"500"}}
			if cursor != "" {
				q.Set("cursor", cursor)
			}
			var page struct {
				Items      []notification `json:"items"`
				NextCursor string         `json:"nextCursor"`
			}
			if err := c.do(ctx, http.MethodGet, c.api+"/v1/notifications?"+q.Encode(), nil, http.StatusOK, &page); err != nil {
				return delivered{}, err
			}
			for _, n := range page.Items {
				if n.SentAt != nil {
					out.sent[n.Channel] = append(out.sent[n.Channel], *n.SentAt)
				}
				if n.QueuedAt != nil && n.QueuedAt.After(out.lastQueued) {
					out.lastQueued = *n.QueuedAt
				}
			}
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
	}
	return out, nil
}

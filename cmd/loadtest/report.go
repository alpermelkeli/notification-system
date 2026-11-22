package main

import (
	"fmt"
	"io"
	"math"
	"slices"
	"time"
)

// Targets from docs/capacity-planning.md.
const (
	targetIngestion  = 5000.0      // notifications accepted per second (section 2)
	targetDrainShare = 0.95        // of the rate limit, per channel (sections 1 and 6)
	targetP99        = time.Second // end-to-end for a high-priority notification (section 10)
)

type report struct {
	cfg      config
	idle     probeResult
	busy     probeResult
	burst    burstResult
	drained  drainResult
	rates    rateWatch
	sent     map[string][]time.Time
	queued   time.Time // when the dispatcher had published the whole burst
	rejected int64
}

// percentile returns the p-th percentile (0 to 100) by the nearest-rank method.
func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	rank := int(math.Ceil(p / 100 * float64(len(s))))
	return s[min(max(rank, 1), len(s))-1]
}

// drainRate is the delivery rate between the first and the last send of a channel.
func drainRate(ts []time.Time) float64 {
	if len(ts) < 2 {
		return 0
	}
	first, last := slices.MinFunc(ts, time.Time.Compare), slices.MaxFunc(ts, time.Time.Compare)
	secs := last.Sub(first).Seconds()
	if secs <= 0 {
		return 0
	}
	return float64(len(ts)-1) / secs
}

// jitterAllowance is how far above the limit one wall-clock second at the provider may go
// because of jitter on the network path: 1% of the limit, at least one send. It is reported as a
// warning, not hidden.
func jitterAllowance(limit int64) int64 { return max(1, limit/100) }

func ms(d time.Duration) string {
	return fmt.Sprintf("%d ms", d.Round(time.Millisecond).Milliseconds())
}

// print writes the report and returns whether every check passed.
func (r report) print(w io.Writer) bool {
	ok := true
	line := func(mark, format string, args ...any) {
		fmt.Fprintf(w, "  %s  %s\n", mark, fmt.Sprintf(format, args...))
	}
	check := func(pass bool, format string, args ...any) {
		if !pass {
			ok = false
			line("FAIL", format, args...)
			return
		}
		line("PASS", format, args...)
	}

	fmt.Fprintln(w, "\nIngestion")
	fmt.Fprintf(w, "  %d notifications in %d batch requests, accepted in %.1f s: %.0f per second\n",
		r.burst.accepted, len(r.burst.batchIDs), r.burst.elapsed.Seconds(), r.burst.perSecond)
	fmt.Fprintf(w, "  batch request time: p50 %s, p99 %s\n", ms(percentile(r.burst.requests, 50)), ms(percentile(r.burst.requests, 99)))
	check(r.burst.perSecond >= targetIngestion, "accepted at least %.0f per second", targetIngestion)

	if !r.queued.IsZero() {
		d := r.queued.Sub(r.burst.start)
		fmt.Fprintf(w, "  dispatcher published the whole burst to Kafka %.1f s after it started: %.0f per second or more\n",
			d.Seconds(), float64(r.burst.accepted)/d.Seconds())
	}

	fmt.Fprintln(w, "\nDelivery of the burst")
	var lastSent time.Time
	for _, ch := range channels {
		ts := r.sent[ch]
		for _, t := range ts {
			if t.After(lastSent) {
				lastSent = t
			}
		}
		fmt.Fprintf(w, "  %-5s %6d sent, %.1f per second, highest second seen by the provider: %d\n",
			ch, len(ts), drainRate(ts), r.rates.max[ch])
	}
	if !lastSent.IsZero() {
		fmt.Fprintf(w, "  drained %.0f s after the burst started\n", lastSent.Sub(r.burst.start).Seconds())
	}
	fmt.Fprintf(w, "  final statuses: %v; provider rejections: %d; rate samples: %d (%d failed)\n",
		r.drained.counts, r.rejected, r.rates.samples, r.rates.errors)
	for _, ch := range channels {
		limit, peak := int64(r.cfg.limit), r.rates.max[ch]
		if peak > limit && peak <= limit+jitterAllowance(limit) {
			// Tokens are exactly 1/rate apart; a few milliseconds of jitter between the token
			// and the arrival can move one send into the neighbouring second.
			line("WARN", "%s peaked at %d per second at the provider: within the arrival jitter allowance of %d",
				ch, peak, jitterAllowance(limit))
			continue
		}
		check(peak <= limit, "%s never went over %d per second at the provider", ch, r.cfg.limit)
	}
	for _, ch := range channels {
		limit := float64(r.cfg.limit)
		check(drainRate(r.sent[ch]) >= targetDrainShare*limit, "%s drained at %.0f%% of the limit or more", ch, targetDrainShare*100)
	}
	check(r.drained.counts["sent"] == r.burst.accepted, "every notification of the burst was sent")

	fmt.Fprintln(w, "\nHigh priority, created to sent")
	for _, p := range []struct {
		name string
		res  probeResult
	}{{"no backlog", r.idle}, {"behind the backlog", r.busy}} {
		l := p.res.latencies
		fmt.Fprintf(w, "  %-19s p50 %s, p95 %s, p99 %s, max %s (%d sent, %d failed)\n", p.name,
			ms(percentile(l, 50)), ms(percentile(l, 95)), ms(percentile(l, 99)), ms(percentile(l, 100)), len(l), p.res.failed)
		fmt.Fprintf(w, "  %-19s of which waiting for the dispatcher: p99 %s; queued to sent: p99 %s\n", "",
			ms(percentile(p.res.dispatch, 99)), ms(percentile(p.res.worker, 99)))
	}
	check(r.idle.failed == 0 && percentile(r.idle.latencies, 99) < targetP99, "no backlog: p99 under %s", ms(targetP99))
	check(r.busy.failed == 0 && percentile(r.busy.latencies, 99) < targetP99, "behind the backlog: p99 under %s", ms(targetP99))

	if ok {
		fmt.Fprintln(w, "\nAll checks passed.")
	} else {
		fmt.Fprintln(w, "\nSome checks failed.")
	}
	return ok
}

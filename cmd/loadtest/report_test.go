package main

import (
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	ds := []time.Duration{5, 1, 4, 2, 3, 10, 9, 8, 7, 6}
	tests := []struct {
		p    float64
		want time.Duration
	}{{0, 1}, {10, 1}, {50, 5}, {95, 10}, {99, 10}, {100, 10}}
	for _, tt := range tests {
		if got := percentile(ds, tt.p); got != tt.want {
			t.Errorf("percentile(%v) = %v, want %v", tt.p, got, tt.want)
		}
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("percentile of nothing = %v, want 0", got)
	}
	if ds[0] != 5 {
		t.Error("percentile sorted its input")
	}
}

func TestDrainRate(t *testing.T) {
	t0 := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	var ts []time.Time
	for i := range 101 { // 101 sends 10 ms apart: 100 intervals in one second
		ts = append(ts, t0.Add(time.Duration(100-i)*10*time.Millisecond))
	}
	if got := drainRate(ts); got < 99.99 || got > 100.01 {
		t.Errorf("drainRate = %v, want 100", got)
	}
	if got := drainRate(ts[:1]); got != 0 {
		t.Errorf("drainRate of one send = %v, want 0", got)
	}
	if got := drainRate([]time.Time{t0, t0}); got != 0 {
		t.Errorf("drainRate of simultaneous sends = %v, want 0", got)
	}
}

func TestConfigValidate(t *testing.T) {
	good := config{total: 3000, batchSize: 1000, concurrency: 1, probes: 1, limit: 100}
	if err := good.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mutate := range map[string]func(*config){
		"batch too big":    func(c *config) { c.batchSize = 1001 },
		"too few":          func(c *config) { c.total = 2999 },
		"no concurrency":   func(c *config) { c.concurrency = 0 },
		"no probes":        func(c *config) { c.probes = 0 },
		"no limit":         func(c *config) { c.limit = 0 },
		"batch of nothing": func(c *config) { c.batchSize = 0 },
	} {
		c := good
		mutate(&c)
		if c.validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRateCheck(t *testing.T) {
	tests := []struct {
		peak int64
		want string
	}{
		{100, "PASS"},
		{101, "WARN"},
		{102, "FAIL"},
	}
	for _, tt := range tests {
		r := report{
			cfg:   config{limit: 100},
			rates: rateWatch{max: map[string]int64{"sms": tt.peak}},
		}
		var b strings.Builder
		r.print(&b)
		var got string
		for _, l := range strings.Split(b.String(), "\n") {
			if strings.Contains(l, "sms") && strings.Contains(l, "at the provider") && !strings.Contains(l, "sent,") {
				got = strings.Fields(l)[0]
			}
		}
		if got != tt.want {
			t.Errorf("peak %d: got %q, want %q\n%s", tt.peak, got, tt.want, b.String())
		}
	}
}

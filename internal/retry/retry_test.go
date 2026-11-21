package retry

import (
	"testing"
	"time"
)

func TestDelayDoublesUpToTheCap(t *testing.T) {
	cases := []struct {
		attempt int
		ceiling time.Duration
	}{
		{0, 5 * time.Second}, // defensive: treated like the first attempt
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{4, 40 * time.Second},
		{7, 5 * time.Minute}, // 320s capped at 300s
		{40, 5 * time.Minute},
		{1000, 5 * time.Minute}, // no overflow
	}
	for _, r := range []float64{0, 0.5, 0.999999} {
		p := Policy{Rand: func() float64 { return r }}.WithDefaults()
		for _, c := range cases {
			got := p.Delay(c.attempt)
			lo, hi := c.ceiling/2, c.ceiling
			if got < lo || got > hi {
				t.Errorf("rand %v attempt %d: Delay = %v, want in [%v, %v]", r, c.attempt, got, lo, hi)
			}
			if r == 0 && got != lo {
				t.Errorf("attempt %d: Delay with rand 0 = %v, want half the ceiling %v", c.attempt, got, lo)
			}
		}
	}
}

func TestDelayIsJittered(t *testing.T) {
	p := Policy{}.WithDefaults()
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[p.Delay(3)] = true
	}
	if len(seen) < 10 {
		t.Errorf("50 delays produced only %d distinct values; jitter is missing", len(seen))
	}
}

func TestWithDefaults(t *testing.T) {
	p := Policy{}.WithDefaults()
	if p.BaseDelay != 5*time.Second || p.MaxDelay != 5*time.Minute || p.MaxAttempts != 5 || p.Rand == nil {
		t.Errorf("defaults = %+v", p)
	}
	p = Policy{BaseDelay: time.Minute, MaxDelay: time.Second}.WithDefaults()
	if p.MaxDelay != time.Minute {
		t.Errorf("MaxDelay below BaseDelay must be raised to it: %v", p.MaxDelay)
	}
}

func TestExhausted(t *testing.T) {
	p := Policy{MaxAttempts: 3}.WithDefaults()
	for attempts, want := range map[int]bool{1: false, 2: false, 3: true, 4: true} {
		if got := p.Exhausted(attempts); got != want {
			t.Errorf("Exhausted(%d) = %v, want %v", attempts, got, want)
		}
	}
}

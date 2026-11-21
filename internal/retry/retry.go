// Package retry is the delivery retry policy of ADR-0005 (FR-3.1): exponential backoff with equal
// jitter and an attempt budget.
package retry

import (
	"math/rand/v2"
	"time"
)

// Policy is the retry policy. Retries are out-of-band: the worker stores the delay as the row's
// next_attempt_at and the dispatcher re-publishes it when due, so nothing sleeps on a message.
type Policy struct {
	// BaseDelay is the backoff ceiling of the first retry. Default 5s.
	BaseDelay time.Duration
	// MaxDelay caps the backoff ceiling. Default 5m.
	MaxDelay time.Duration
	// MaxAttempts is the attempt budget; the failure of the last attempt is final. Default 5.
	MaxAttempts int
	// Rand returns a uniform number in [0, 1). Default math/rand/v2.
	Rand func() float64
}

// Default returns the defaults of ADR-0005.
func Default() Policy {
	return Policy{BaseDelay: 5 * time.Second, MaxDelay: 5 * time.Minute, MaxAttempts: 5}
}

// WithDefaults returns p with zero fields replaced by the defaults.
func (p Policy) WithDefaults() Policy {
	d := Default()
	if p.BaseDelay <= 0 {
		p.BaseDelay = d.BaseDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = d.MaxDelay
	}
	p.MaxDelay = max(p.MaxDelay, p.BaseDelay)
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = d.MaxAttempts
	}
	if p.Rand == nil {
		p.Rand = rand.Float64
	}
	return p
}

// Exhausted reports whether a notification that has made attempts attempts has no budget left.
func (p Policy) Exhausted(attempts int) bool { return attempts >= p.MaxAttempts }

// Delay returns the wait before the retry that follows failed attempt number attempt (1-based):
//
//	d = min(BaseDelay · 2^(attempt-1), MaxDelay)
//	delay = d/2 + rand(0, d/2)
//
// Equal jitter keeps at least half the backoff, so a retry never comes back immediately, and
// spreads the rest, so a burst of notifications that failed together does not retry together.
// The policy must have been through WithDefaults.
func (p Policy) Delay(attempt int) time.Duration {
	d := p.MaxDelay
	if shift := attempt - 1; shift < 0 {
		d = p.BaseDelay
	} else if shift < 32 && p.BaseDelay<<shift > 0 && p.BaseDelay<<shift < p.MaxDelay {
		// shift < 32 and the overflow check keep the doubling from wrapping around.
		d = p.BaseDelay << shift
	}
	half := d / 2
	return half + time.Duration(p.Rand()*float64(d-half))
}

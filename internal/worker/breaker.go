package worker

import (
	"errors"
	"log/slog"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/provider"
)

// BreakerOptions configures the per-channel circuit breaker of ADR-0005. Zero fields take the
// defaults.
type BreakerOptions struct {
	// ConsecutiveFailures opens the breaker after this many retryable failures in a row. Default 5.
	ConsecutiveFailures int
	// FailureRatio and MinRequests open the breaker when more than FailureRatio of at least
	// MinRequests counted sends in the last Window failed. Defaults 0.5 and 20.
	FailureRatio float64
	MinRequests  int
	// Window is the rolling window of the ratio rule. Default 1m.
	Window time.Duration
	// OpenTimeout is how long the breaker stays open before a single probe is let through.
	// Released notifications are retried after it. Default 30s.
	OpenTimeout time.Duration
}

func (o *BreakerOptions) defaults() {
	setDefault(&o.ConsecutiveFailures, 5)
	setDefault(&o.MinRequests, 20)
	setDefault(&o.Window, time.Minute)
	setDefault(&o.OpenTimeout, 30*time.Second)
	if o.FailureRatio <= 0 || o.FailureRatio >= 1 {
		o.FailureRatio = 0.5
	}
}

// errNotAttempted reports to the breaker that an admitted send did not happen (stale record, no
// token, lost partition). It counts neither way, so a half-open breaker admits the next probe.
var errNotAttempted = errors.New("send not attempted")

// breakerPoll is how often a worker checks whether a non-admitting breaker admits again. The
// open → half-open transition happens on the first check after OpenTimeout.
const breakerPoll = 100 * time.Millisecond

// circuit wraps a two-step breaker. Only retryable failures (no answer, 5xx, 408, 425) count as
// failures: a permanent 4xx is a healthy provider rejecting one request, and a 429 is
// backpressure, which the rate limiter's cool-down handles.
type circuit struct {
	cb      *gobreaker.TwoStepCircuitBreaker[struct{}]
	timeout time.Duration
}

// newCircuit returns a breaker; onChange, if set, receives every state change as the metrics
// gauge value and the new state's name.
func newCircuit(name string, o BreakerOptions, logger *slog.Logger, onChange func(state int, to string)) *circuit {
	return &circuit{
		timeout: o.OpenTimeout,
		cb: gobreaker.NewTwoStepCircuitBreaker[struct{}](gobreaker.Settings{
			Name:        name,
			MaxRequests: 1, // one probe while half-open
			Interval:    o.Window,
			// Six buckets make the window roll instead of resetting all at once.
			BucketPeriod: o.Window / 6,
			Timeout:      o.OpenTimeout,
			ReadyToTrip: func(c gobreaker.Counts) bool {
				if c.ConsecutiveFailures >= uint32(o.ConsecutiveFailures) { //nolint:gosec // validated positive
					return true
				}
				counted := c.TotalSuccesses + c.TotalFailures
				return counted >= uint32(o.MinRequests) && //nolint:gosec // validated positive
					float64(c.TotalFailures) > o.FailureRatio*float64(counted)
			},
			IsSuccessful: func(err error) bool {
				c := provider.Classify(err)
				return c == provider.ClassDelivered || c == provider.ClassPermanent
			},
			IsExcluded: func(err error) bool {
				return errors.Is(err, errNotAttempted) || provider.Classify(err) == provider.ClassRateLimited
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				log := logger.Warn
				if to == gobreaker.StateClosed {
					log = logger.Info
				}
				log("circuit breaker state changed", "breaker", name, "from", from.String(), "to", to.String())
				if onChange != nil {
					onChange(gaugeValue(to), to.String())
				}
			},
		}),
	}
}

// gaugeValue maps a breaker state to the circuit_breaker_state gauge.
func gaugeValue(s gobreaker.State) int {
	switch s {
	case gobreaker.StateOpen:
		return metrics.BreakerOpen
	case gobreaker.StateHalfOpen:
		return metrics.BreakerHalfOpen
	default:
		return metrics.BreakerClosed
	}
}

// admission is the breaker's permission for one send. done must be called exactly once with the
// send's error, or errNotAttempted if the send did not happen. probe tells whether it was
// admitted as the single half-open probe.
type admission struct {
	done  func(error)
	probe bool
}

// allow admits one send.
func (c *circuit) allow() (admission, error) {
	done, err := c.cb.Allow()
	if err != nil {
		return admission{}, err
	}
	return admission{done: done, probe: c.cb.State() == gobreaker.StateHalfOpen}, nil
}

// revoked reports whether a send admitted earlier must not go ahead any more: a probe once the
// breaker is open again, any other send once the breaker is no longer closed. So a send admitted
// before a trip and delayed (e.g. by a slow claim) does not reach the provider next to the probe.
func (c *circuit) revoked(a admission) bool {
	st := c.cb.State()
	if a.probe {
		return st == gobreaker.StateOpen
	}
	return st != gobreaker.StateClosed
}

// open reports whether the breaker rejects sends right now.
func (c *circuit) open() bool { return c.cb.State() == gobreaker.StateOpen }

// state returns the breaker state for logs and tests.
func (c *circuit) state() gobreaker.State { return c.cb.State() }

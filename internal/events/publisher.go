// Package events publishes status events to notification.events (ADR-0011). Publishing is best
// effort and never blocks the caller: the transition is already committed, PostgreSQL is the
// source of truth, and a lost event only delays a WebSocket update.
package events

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/telemetry"
)

// Publisher publishes the status events of changed rows. It is safe for concurrent use, and a nil
// *Publisher publishes nothing.
type Publisher struct {
	cl      *kgo.Client
	metrics *metrics.Events
	logger  *slog.Logger
}

// NewPublisher returns a publisher on cl, a producing client (dispatcher.ProducerOptions bounds
// how long a record may wait for its acknowledgement). m may be nil.
func NewPublisher(cl *kgo.Client, m *metrics.Events, logger *slog.Logger) *Publisher {
	return &Publisher{cl: cl, metrics: m, logger: logger}
}

// Publish publishes one event per row, each describing the status the row has just moved to. It
// returns at once: records are buffered and sent in the background, and a record that cannot be
// buffered (the buffer is full, e.g. while Kafka is down) is dropped. Call it only after the
// transition is committed.
func (p *Publisher) Publish(ctx context.Context, ns ...domain.Notification) {
	if p == nil {
		return
	}
	// The records outlive the caller: a request context ends as soon as the response is written.
	ctx = context.WithoutCancel(ctx)
	for _, n := range ns {
		r, err := queue.NewStatusEvent(n).Record(n.CorrelationID)
		if err != nil {
			p.metrics.Result(metrics.EventFailed)
			p.logger.WarnContext(ctx, "status event not encoded", "notification_id", n.ID, "err", err)
			continue
		}
		telemetry.InjectRecord(ctx, r)
		p.cl.TryProduce(ctx, r, p.done)
	}
}

func (p *Publisher) done(r *kgo.Record, err error) {
	switch {
	case err == nil:
		p.metrics.Result(metrics.EventPublished)
	case errors.Is(err, kgo.ErrMaxBuffered):
		p.metrics.Result(metrics.EventDropped)
		p.logger.Debug("status event dropped: producer buffer full", "key", string(r.Key))
	default:
		p.metrics.Result(metrics.EventFailed)
		p.logger.Debug("status event not published", "key", string(r.Key), "err", err)
	}
}

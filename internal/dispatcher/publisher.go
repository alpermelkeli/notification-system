// Package dispatcher relays due notifications from PostgreSQL to the Kafka work topics and
// recovers rows that never reach a worker (ADR-0003). Any number of dispatcher replicas can run:
// they coordinate only through row locks (FOR UPDATE SKIP LOCKED).
package dispatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/storage"
	"github.com/alpermelkeli/notification-system/internal/telemetry"
)

// Publisher publishes claim-check work messages (ADR-0002). The franz-go client is an
// idempotent producer with acks=all by default, which the options below make explicit.
type Publisher struct {
	cl      *kgo.Client
	timeout time.Duration
	metrics *metrics.Dispatch
}

// PublishTimeout bounds one batch publish. The dispatcher holds the batch's row locks while it
// publishes, and franz-go retries forever by default, so without a bound an unavailable partition
// would keep up to BatchSize rows locked: their cancels would time out and the workers' claims of
// records that did land would wait on the locks.
const PublishTimeout = 10 * time.Second

// ProducerOptions are the kgo options a publishing client must use.
func ProducerOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Fail records that cannot be delivered in time instead of retrying forever.
		kgo.RecordDeliveryTimeout(PublishTimeout),
		// Records are keyed by notification id; the default partitioner hashes keys like the Java
		// client, so a notification's records always land on the same partition.
	}
}

// NewPublisher returns a publisher on cl, which must be built with ProducerOptions. m may be nil.
func NewPublisher(cl *kgo.Client, m *metrics.Dispatch) *Publisher {
	return &Publisher{cl: cl, timeout: PublishTimeout, metrics: m}
}

// Publish implements storage.PublishFunc: it produces one record per notification, waits until
// all are acknowledged and returns where each landed, in input order. If any record fails, the
// whole batch fails and the dispatcher's transaction rolls back; records that did land become
// duplicates, which the workers' guarded claim discards (ADR-0004).
//
// Each record gets a producer span, a child of the request that created the notification (its
// stored traceparent), and carries that span's context in its headers to the worker (B-5).
func (p *Publisher) Publish(ctx context.Context, ns []domain.Notification) ([]storage.KafkaPosition, error) {
	records := make([]*kgo.Record, len(ns))
	spans := make([]trace.Span, len(ns))
	index := make(map[*kgo.Record]int, len(ns))
	defer func() {
		for _, s := range spans {
			if s != nil {
				s.End()
			}
		}
	}()
	for i, n := range ns {
		r, err := queue.NewMessage(n).Record(n.CorrelationID)
		if err != nil {
			return nil, err
		}
		sctx, span := startPublishSpan(ctx, n, r.Topic)
		telemetry.InjectRecord(sctx, r)
		records[i], spans[i] = r, span
		index[r] = i
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	results := p.cl.ProduceSync(ctx, records...)
	// Results arrive in completion order, not input order.
	positions := make([]storage.KafkaPosition, len(ns))
	for _, res := range results {
		i := index[res.Record]
		if res.Err != nil {
			spans[i].RecordError(res.Err)
			spans[i].SetStatus(codes.Error, "produce failed")
			continue
		}
		positions[i] = storage.KafkaPosition{Partition: res.Record.Partition, Offset: res.Record.Offset}
		spans[i].SetAttributes(
			semconv.MessagingDestinationPartitionID(fmt.Sprint(res.Record.Partition)),
			semconv.MessagingKafkaOffset(int(res.Record.Offset)),
		)
	}
	if err := results.FirstErr(); err != nil {
		return nil, fmt.Errorf("produce %d work records: %w", len(records), err)
	}
	p.metrics.Dispatched(ns)
	return positions, nil
}

// startPublishSpan starts the producer span of one dispatch as a child of the notification's
// stored trace context.
func startPublishSpan(ctx context.Context, n domain.Notification, topic string) (context.Context, trace.Span) {
	return telemetry.Tracer().Start(telemetry.WithTraceParent(ctx, n.TraceParent), "send "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeSend,
			semconv.MessagingDestinationName(topic),
			telemetry.AttrNotificationID.String(n.ID.String()),
			telemetry.AttrChannel.String(string(n.Channel)),
			telemetry.AttrPriority.String(string(n.Priority)),
			telemetry.AttrAttempt.Int(n.Attempts+1),
			telemetry.AttrCorrelationID.String(n.CorrelationID),
		))
}

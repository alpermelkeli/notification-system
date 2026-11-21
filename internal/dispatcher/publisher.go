// Package dispatcher relays due notifications from PostgreSQL to the Kafka work topics and
// recovers rows that never reach a worker (ADR-0003). Any number of dispatcher replicas can run:
// they coordinate only through row locks (FOR UPDATE SKIP LOCKED).
package dispatcher

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// Publisher publishes claim-check work messages (ADR-0002). The franz-go client is an
// idempotent producer with acks=all by default, which the options below make explicit.
type Publisher struct {
	cl *kgo.Client
}

// ProducerOptions are the kgo options a publishing client must use.
func ProducerOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Records are keyed by notification id; the default partitioner hashes keys like the Java
		// client, so a notification's records always land on the same partition.
	}
}

// NewPublisher returns a publisher on cl, which must be built with ProducerOptions.
func NewPublisher(cl *kgo.Client) *Publisher { return &Publisher{cl: cl} }

// Publish implements storage.PublishFunc: it produces one record per notification, waits until
// all are acknowledged and returns where each landed, in input order. If any record fails, the
// whole batch fails and the dispatcher's transaction rolls back; records that did land become
// duplicates, which the workers' guarded claim discards (ADR-0004).
func (p *Publisher) Publish(ctx context.Context, ns []domain.Notification) ([]storage.KafkaPosition, error) {
	records := make([]*kgo.Record, len(ns))
	index := make(map[*kgo.Record]int, len(ns))
	for i, n := range ns {
		r, err := queue.NewMessage(n).Record(n.CorrelationID)
		if err != nil {
			return nil, err
		}
		records[i] = r
		index[r] = i
	}
	results := p.cl.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return nil, fmt.Errorf("produce %d work records: %w", len(records), err)
	}
	// Results arrive in completion order, not input order.
	positions := make([]storage.KafkaPosition, len(ns))
	for _, res := range results {
		positions[index[res.Record]] = storage.KafkaPosition{Partition: res.Record.Partition, Offset: res.Record.Offset}
	}
	return positions, nil
}

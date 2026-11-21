package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// DeadLetterTopic receives one record per notification that ended in failed (ADR-0005, FR-3.3).
const DeadLetterTopic = "notifications.dlq"

// DeadLetter is the record published when a notification fails for good. Like the work message it
// is a claim-check: the recipient and content stay in PostgreSQL, so the topic holds no personal
// data and can be retained and shared freely. The row is the source of truth; the topic is an
// audit feed for alerting and external tooling, and replay goes through
// POST /v1/notifications/{id}/retry.
type DeadLetter struct {
	ID            uuid.UUID       `json:"id"`
	CreatedAt     time.Time       `json:"created_at"`
	BatchID       *uuid.UUID      `json:"batch_id,omitempty"`
	Channel       domain.Channel  `json:"channel"`
	Priority      domain.Priority `json:"priority"`
	Attempts      int             `json:"attempts"`
	LastError     string          `json:"last_error"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	FailedAt      time.Time       `json:"failed_at"`
}

// NewDeadLetter builds the dead letter of a notification that has just moved to failed.
func NewDeadLetter(n domain.Notification) DeadLetter {
	d := DeadLetter{
		ID: n.ID, CreatedAt: n.CreatedAt, BatchID: n.BatchID, Channel: n.Channel, Priority: n.Priority,
		Attempts: n.Attempts, CorrelationID: n.CorrelationID, FailedAt: n.UpdatedAt,
	}
	if n.LastError != nil {
		d.LastError = *n.LastError
	}
	return d
}

// Record encodes d as a record on DeadLetterTopic, keyed by the notification id.
func (d DeadLetter) Record() (*kgo.Record, error) {
	value, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("encode dead letter: %w", err)
	}
	r := &kgo.Record{Topic: DeadLetterTopic, Key: []byte(d.ID.String()), Value: value}
	if d.CorrelationID != "" {
		r.Headers = []kgo.RecordHeader{{Key: HeaderCorrelationID, Value: []byte(d.CorrelationID)}}
	}
	return r, nil
}

// DeadLetters publishes dead letters. Publishing is best effort: the failed row is already
// committed and visible through the API, so a lost dead letter loses only the audit record.
type DeadLetters struct {
	cl      *kgo.Client
	timeout time.Duration
}

// NewDeadLetters returns a dead-letter publisher on cl, which must be a producing client. timeout
// bounds each publish.
func NewDeadLetters(cl *kgo.Client, timeout time.Duration) *DeadLetters {
	return &DeadLetters{cl: cl, timeout: timeout}
}

// Publish publishes the dead letters of ns, which must have just moved to failed, and waits for
// the broker's acknowledgement.
func (p *DeadLetters) Publish(ctx context.Context, ns ...domain.Notification) error {
	if len(ns) == 0 {
		return nil
	}
	records := make([]*kgo.Record, len(ns))
	for i, n := range ns {
		r, err := NewDeadLetter(n).Record()
		if err != nil {
			return err
		}
		records[i] = r
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if err := p.cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("publish %d dead letters: %w", len(records), err)
	}
	return nil
}

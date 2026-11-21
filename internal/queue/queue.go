// Package queue is the Kafka contract shared by the dispatcher and the workers (ADR-0002): topic
// and consumer group names, and the claim-check work message. Nothing else in the codebase builds
// topic names or encodes work records.
package queue

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// HeaderCorrelationID carries the correlation ID of the request that created the notification,
// so worker logs and provider calls can be traced back to it (FR-4.2).
const HeaderCorrelationID = "correlation_id"

const topicPrefix = "notifications."

// Topic returns the work topic of a channel and priority, e.g. notifications.sms.high.
func Topic(c domain.Channel, p domain.Priority) string {
	return topicPrefix + string(c) + "." + string(p)
}

// Topics returns the three work topics of a channel, highest priority first.
func Topics(c domain.Channel) []string {
	out := make([]string, len(domain.Priorities))
	for i, p := range domain.Priorities {
		out[i] = Topic(c, p)
	}
	return out
}

// ParseTopic is the inverse of Topic. It reports false for any other topic.
func ParseTopic(topic string) (domain.Channel, domain.Priority, bool) {
	rest, ok := strings.CutPrefix(topic, topicPrefix)
	if !ok {
		return "", "", false
	}
	cs, ps, ok := strings.Cut(rest, ".")
	if !ok {
		return "", "", false
	}
	c, err := domain.ParseChannel(cs)
	if err != nil {
		return "", "", false
	}
	p, err := domain.ParsePriority(ps)
	if err != nil || ps == "" {
		return "", "", false
	}
	return c, p, true
}

// Group returns the consumer group of a channel's workers, e.g. workers.sms.
func Group(c domain.Channel) string { return "workers." + string(c) }

// Message is the claim-check published for each dispatch of a notification. It carries only what
// routing, scheduling and the claim need; the recipient and content are read from PostgreSQL by
// the claim, so the record can never diverge from the source of truth.
type Message struct {
	ID uuid.UUID `json:"id"`
	// CreatedAt is the partition key of the row, so the claim prunes to one partition.
	CreatedAt time.Time       `json:"created_at"`
	Channel   domain.Channel  `json:"channel"`
	Priority  domain.Priority `json:"priority"`
	// Attempt is the attempt this dispatch is for (1 for the first delivery). Informational only:
	// it never changes scheduling (ADR-0006).
	Attempt int `json:"attempt"`
	// DueAt is when the notification became due. The scheduler measures waiting time from it.
	DueAt time.Time `json:"due_at"`
}

// NewMessage builds the work message for a notification that is being dispatched.
func NewMessage(n domain.Notification) Message {
	return Message{
		ID: n.ID, CreatedAt: n.CreatedAt, Channel: n.Channel, Priority: n.Priority,
		Attempt: n.Attempts + 1, DueAt: n.NextAttemptAt,
	}
}

// Record encodes m as a Kafka record on its work topic, keyed by the notification id.
func (m Message) Record(correlationID string) (*kgo.Record, error) {
	value, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode work message: %w", err)
	}
	r := &kgo.Record{Topic: Topic(m.Channel, m.Priority), Key: []byte(m.ID.String()), Value: value}
	if correlationID != "" {
		r.Headers = []kgo.RecordHeader{{Key: HeaderCorrelationID, Value: []byte(correlationID)}}
	}
	return r, nil
}

// Decode parses a work record and checks that it is consistent with its topic. A record that
// fails to decode can never be processed and should be skipped.
func Decode(r *kgo.Record) (Message, error) {
	var m Message
	if err := json.Unmarshal(r.Value, &m); err != nil {
		return Message{}, fmt.Errorf("decode work message: %w", err)
	}
	c, p, ok := ParseTopic(r.Topic)
	switch {
	case !ok:
		return Message{}, fmt.Errorf("decode work message: %q is not a work topic", r.Topic)
	case m.Channel != c || m.Priority != p:
		return Message{}, fmt.Errorf("decode work message: %s/%s does not match topic %s", m.Channel, m.Priority, r.Topic)
	case !domain.IsV7(m.ID) || m.CreatedAt.IsZero():
		return Message{}, fmt.Errorf("decode work message: invalid id %s or created_at", m.ID)
	}
	return m, nil
}

// CorrelationID returns the correlation ID header of r, or "".
func CorrelationID(r *kgo.Record) string {
	for _, h := range r.Headers {
		if h.Key == HeaderCorrelationID {
			return string(h.Value)
		}
	}
	return ""
}

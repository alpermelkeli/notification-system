package queue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// EventsTopic receives a status event for every transition of a notification (ADR-0011). Events
// are published best effort after the commit, by the component that made the change; PostgreSQL
// stays the source of truth.
const EventsTopic = "notification.events"

// StatusEvent is the record published when a notification changes status. Its JSON form is sent
// to WebSocket clients unchanged, so it uses the API's field names. It never carries the
// recipient or the content.
type StatusEvent struct {
	NotificationID uuid.UUID       `json:"notificationId"`
	BatchID        *uuid.UUID      `json:"batchId,omitempty"`
	Channel        domain.Channel  `json:"channel"`
	Priority       domain.Priority `json:"priority"`
	Status         domain.Status   `json:"status"`
	Attempts       int             `json:"attempts"`
	// NextAttemptAt is set while the notification waits for dispatch (pending, retrying).
	NextAttemptAt *time.Time `json:"nextAttemptAt,omitempty"`
	LastError     *string    `json:"lastError,omitempty"`
	// At is the row's updated_at: the database time of the transition. Events from different
	// components can arrive out of order; clients keep the latest At per notification.
	At time.Time `json:"at"`
}

// NewStatusEvent builds the event of a notification row that has just changed status.
func NewStatusEvent(n domain.Notification) StatusEvent {
	e := StatusEvent{
		NotificationID: n.ID, BatchID: n.BatchID, Channel: n.Channel, Priority: n.Priority,
		Status: n.Status, Attempts: n.Attempts, At: n.UpdatedAt,
	}
	switch n.Status {
	case domain.StatusPending, domain.StatusRetrying:
		next := n.NextAttemptAt
		e.NextAttemptAt = &next
	}
	switch n.Status {
	case domain.StatusRetrying, domain.StatusFailed:
		e.LastError = n.LastError
	}
	return e
}

// Record encodes e as a record on EventsTopic, keyed by the notification id so a notification's
// events from one producer stay in order.
func (e StatusEvent) Record(correlationID string) (*kgo.Record, error) {
	value, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode status event: %w", err)
	}
	r := &kgo.Record{Topic: EventsTopic, Key: []byte(e.NotificationID.String()), Value: value}
	if correlationID != "" {
		r.Headers = []kgo.RecordHeader{{Key: HeaderCorrelationID, Value: []byte(correlationID)}}
	}
	return r, nil
}

// DecodeStatusEvent parses a record of EventsTopic.
func DecodeStatusEvent(r *kgo.Record) (StatusEvent, error) {
	var e StatusEvent
	if err := json.Unmarshal(r.Value, &e); err != nil {
		return StatusEvent{}, fmt.Errorf("decode status event: %w", err)
	}
	if e.NotificationID == uuid.Nil || e.Status == "" {
		return StatusEvent{}, fmt.Errorf("decode status event: missing notification id or status")
	}
	return e, nil
}

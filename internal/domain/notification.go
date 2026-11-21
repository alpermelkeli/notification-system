// Package domain holds the core types of the notification system: channels, priorities, the
// status state machine (ADR-0005) and domain errors. It has no I/O dependencies.
package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Channel is a delivery channel.
type Channel string

// Supported channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel in a stable order.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// ParseChannel validates s as a Channel.
func ParseChannel(s string) (Channel, error) {
	switch c := Channel(s); c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return c, nil
	}
	return "", fmt.Errorf("%w: unknown channel %q", ErrInvalidArgument, s)
}

// Priority is a delivery priority (FR-2.3). It is enforced by the worker scheduler (ADR-0006).
type Priority string

// Supported priorities.
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Priorities lists every priority from highest to lowest.
var Priorities = []Priority{PriorityHigh, PriorityNormal, PriorityLow}

// ParsePriority validates s as a Priority. An empty string means PriorityNormal.
func ParsePriority(s string) (Priority, error) {
	switch p := Priority(s); p {
	case "":
		return PriorityNormal, nil
	case PriorityHigh, PriorityNormal, PriorityLow:
		return p, nil
	}
	return "", fmt.Errorf("%w: unknown priority %q", ErrInvalidArgument, s)
}

// Notification is one message to one recipient on one channel. PostgreSQL is its source of
// truth (ADR-0001); the row also serves as the dispatch outbox (ADR-0003).
type Notification struct {
	// ID is a UUIDv7. CreatedAt is the millisecond timestamp embedded in it (see NewIDAt), which
	// lets any lookup by id prune to a single daily partition.
	ID        uuid.UUID
	CreatedAt time.Time
	// BatchID is set when the notification was created through the batch endpoint.
	BatchID *uuid.UUID

	Channel   Channel
	Priority  Priority
	Recipient string
	Content   string

	Status   Status
	Attempts int
	// NextAttemptAt is when the dispatcher may publish the notification next. It drives initial
	// dispatch, scheduled delivery and retry backoff (ADR-0001).
	NextAttemptAt time.Time
	// ScheduledAt is the requested delivery time of a scheduled notification (B-2).
	ScheduledAt *time.Time
	QueuedAt    *time.Time
	// LeaseUntil bounds how long a worker may hold a notification in StatusProcessing.
	LeaseUntil *time.Time
	// LeaseToken is a fencing token issued by each claim; outcome transitions must present it
	// (see Lease). It is set only while the notification is in StatusProcessing.
	LeaseToken        *uuid.UUID
	SentAt            *time.Time
	ProviderMessageID *string
	LastError         *string

	CorrelationID string
	UpdatedAt     time.Time
}

// Lease identifies one claim of a notification by a worker. Every outcome transition is guarded
// on the token, so a worker whose lease expired and was reaped cannot overwrite the outcome of
// a later claim of the same notification (ABA), even if that claim is processing again.
type Lease struct {
	NotificationID uuid.UUID
	CreatedAt      time.Time
	Token          uuid.UUID
}

// Lease returns the lease of a claimed notification, or false if it is not in StatusProcessing.
func (n Notification) Lease() (Lease, bool) {
	if n.Status != StatusProcessing || n.LeaseToken == nil {
		return Lease{}, false
	}
	return Lease{NotificationID: n.ID, CreatedAt: n.CreatedAt, Token: *n.LeaseToken}, true
}

// Batch groups notifications created by one batch request (FR-1.2).
type Batch struct {
	ID            uuid.UUID
	CreatedAt     time.Time
	Total         int
	CorrelationID string
	// StatusCounts is filled when the batch is read back (FR-1.3).
	StatusCounts map[Status]int
}

// StatusChange is one row of a notification's status history.
type StatusChange struct {
	From    *Status
	To      Status
	Attempt int
	Reason  string
	At      time.Time
}

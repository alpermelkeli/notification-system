package domain

import "fmt"

// Status is the lifecycle state of a notification (FR-3.4, ADR-0005).
//
//	           ┌──────────── cancel ─────────────┐
//	           │                                 ▼
//	pending ──► queued ──► processing ──► sent   cancelled
//	  ▲  (dispatcher)  (worker claim)  │
//	  │                                ├──► retrying ──(due)──► queued
//	  │                                └──► failed ──(replay)──► pending
//	  └── scheduled rows are pending with a future next_attempt_at
type Status string

// Notification statuses.
const (
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Statuses lists every status in lifecycle order.
var Statuses = []Status{
	StatusPending, StatusQueued, StatusProcessing, StatusRetrying,
	StatusSent, StatusFailed, StatusCancelled,
}

// ParseStatus validates s as a Status.
func ParseStatus(s string) (Status, error) {
	for _, st := range Statuses {
		if string(st) == s {
			return st, nil
		}
	}
	return "", fmt.Errorf("%w: unknown status %q", ErrInvalidArgument, s)
}

// transitions is the complete state machine. Every guarded UPDATE in the storage layer must
// correspond to an edge here (enforced by a test in internal/storage).
var transitions = map[Status][]Status{
	// The dispatcher publishes a due row; the client cancels it.
	StatusPending: {StatusQueued, StatusCancelled},
	// A worker claims it; the client cancels it (the worker's guarded claim then drops the Kafka
	// record); the reaper re-publishes a record that never reached a worker.
	StatusQueued: {StatusProcessing, StatusCancelled, StatusPending},
	// The provider outcome, or the reaper after an expired lease (to retrying).
	StatusProcessing: {StatusSent, StatusRetrying, StatusFailed},
	// The dispatcher re-publishes once next_attempt_at is due; the client cancels it.
	StatusRetrying: {StatusQueued, StatusCancelled},
	// Manual replay of a dead letter with a fresh attempt budget (B-1).
	StatusFailed: {StatusPending},
	StatusSent:   nil,
	// Cancelled is terminal.
	StatusCancelled: nil,
}

// CanTransition reports whether the state machine allows from → to.
func CanTransition(from, to Status) bool {
	for _, next := range transitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// IsTerminal reports whether no further transition is possible, apart from the manual replay of
// StatusFailed.
func (s Status) IsTerminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

// CancellableStatuses are the statuses from which a client may cancel (FR-1.4).
var CancellableStatuses = []Status{StatusPending, StatusQueued, StatusRetrying}

// DispatchableStatuses are the statuses the dispatcher publishes once next_attempt_at is due.
var DispatchableStatuses = []Status{StatusPending, StatusRetrying}

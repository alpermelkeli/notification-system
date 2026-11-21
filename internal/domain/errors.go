package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors. The transport layer maps them to HTTP status codes; nothing below it knows
// about HTTP.
var (
	// ErrNotFound means the requested resource does not exist.
	ErrNotFound = errors.New("not found")
	// ErrInvalidArgument means the caller supplied an invalid value.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrInvalidTransition means a guarded status change did not apply because the notification
	// is not in an allowed source status (see TransitionError).
	ErrInvalidTransition = errors.New("invalid status transition")
	// ErrLeaseLost means an outcome was reported with a lease that is no longer current: the
	// claim expired and the notification was reaped, and possibly claimed again.
	ErrLeaseLost = errors.New("lease lost")
	// ErrIdempotencyKeyReused means an Idempotency-Key was reused with a different request
	// (ADR-0004).
	ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different request")
)

// TransitionError reports a rejected guarded transition and the status the notification was
// actually in. It matches ErrInvalidTransition with errors.Is.
type TransitionError struct {
	Current Status
	Target  Status
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s: cannot move from %s to %s", ErrInvalidTransition, e.Current, e.Target)
}

// Unwrap lets errors.Is(err, ErrInvalidTransition) succeed.
func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }

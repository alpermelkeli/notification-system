package domain

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCanTransition(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusQueued}:      true,
		{StatusPending, StatusCancelled}:   true,
		{StatusQueued, StatusProcessing}:   true,
		{StatusQueued, StatusCancelled}:    true,
		{StatusQueued, StatusPending}:      true,
		{StatusProcessing, StatusSent}:     true,
		{StatusProcessing, StatusRetrying}: true,
		{StatusProcessing, StatusFailed}:   true,
		{StatusRetrying, StatusQueued}:     true,
		{StatusRetrying, StatusCancelled}:  true,
		{StatusFailed, StatusPending}:      true,
	}
	// Check every pair, so an accidental extra edge is caught as well as a missing one.
	for _, from := range Statuses {
		for _, to := range Statuses {
			want := allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestTerminalStatusesHaveNoExitExceptReplay(t *testing.T) {
	for _, s := range Statuses {
		if !s.IsTerminal() {
			continue
		}
		for _, to := range Statuses {
			if CanTransition(s, to) && (s != StatusFailed || to != StatusPending) {
				t.Errorf("terminal %s can move to %s", s, to)
			}
		}
	}
}

func TestStatusSetsFollowTheStateMachine(t *testing.T) {
	for _, s := range CancellableStatuses {
		if !CanTransition(s, StatusCancelled) {
			t.Errorf("%s is cancellable but cannot move to cancelled", s)
		}
	}
	for _, s := range DispatchableStatuses {
		if !CanTransition(s, StatusQueued) {
			t.Errorf("%s is dispatchable but cannot move to queued", s)
		}
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		parse   func(string) (string, error)
		in      string
		want    string
		wantErr bool
	}{
		{"channel sms", wrap(ParseChannel), "sms", "sms", false},
		{"channel unknown", wrap(ParseChannel), "fax", "", true},
		{"channel is case sensitive", wrap(ParseChannel), "SMS", "", true},
		{"priority default", wrap(ParsePriority), "", "normal", false},
		{"priority high", wrap(ParsePriority), "high", "high", false},
		{"priority unknown", wrap(ParsePriority), "urgent", "", true},
		{"status failed", wrap(ParseStatus), "failed", "failed", false},
		{"status unknown", wrap(ParseStatus), "done", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.parse(tt.in)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidArgument) {
					t.Fatalf("err = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func wrap[T ~string](f func(string) (T, error)) func(string) (string, error) {
	return func(s string) (string, error) {
		v, err := f(s)
		return string(v), err
	}
}

func TestNewIDAt(t *testing.T) {
	at := time.Date(2026, 11, 21, 23, 59, 59, 987_654_321, time.UTC)
	seen := make(map[uuid.UUID]bool)
	for range 1000 {
		id := NewIDAt(at)
		if !IsV7(id) {
			t.Fatalf("%s is not a v7 UUID (version %d, variant %v)", id, id.Version(), id.Variant())
		}
		if got, want := IDTime(id), at.Truncate(time.Millisecond); !got.Equal(want) {
			t.Fatalf("IDTime = %v, want %v", got, want)
		}
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
}

func TestIDsSortByTime(t *testing.T) {
	a := NewIDAt(time.UnixMilli(1_000))
	b := NewIDAt(time.UnixMilli(1_001))
	if a.String() >= b.String() {
		t.Errorf("%s should sort before %s", a, b)
	}
}

func TestNewBatchItemIDKeepsRequestOrder(t *testing.T) {
	at := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	prev := ""
	for seq := range MaxBatchSeq + 1 {
		id := NewBatchItemID(at, seq)
		if !IsV7(id) || !IDTime(id).Equal(at) || BatchSeq(id) != seq {
			t.Fatalf("seq %d: %s is not a v7 id at %v", seq, id, at)
		}
		if s := id.String(); s <= prev {
			t.Fatalf("seq %d: %s does not sort after %s", seq, s, prev)
		} else {
			prev = s
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("expected a panic for an out-of-range sequence")
		}
	}()
	NewBatchItemID(at, MaxBatchSeq+1)
}

func TestIDTimeMatchesGoogleUUIDv7(t *testing.T) {
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	sec, nsec := id.Time().UnixTime()
	if want := time.Unix(sec, nsec).Truncate(time.Millisecond); !IDTime(id).Equal(want) {
		t.Errorf("IDTime = %v, uuid says %v", IDTime(id), want)
	}
	if IsV7(uuid.New()) {
		t.Error("a v4 UUID must not be reported as v7")
	}
}

func TestTransitionError(t *testing.T) {
	var err error = &TransitionError{Current: StatusSent, Target: StatusCancelled}
	err = fmt.Errorf("cancel: %w", err)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Error("TransitionError should match ErrInvalidTransition")
	}
	var te *TransitionError
	if !errors.As(err, &te) || te.Current != StatusSent {
		t.Errorf("errors.As failed: %v", err)
	}
}

func TestNotificationLease(t *testing.T) {
	token := uuid.New()
	n := Notification{ID: NewIDAt(time.Now()), Status: StatusProcessing, LeaseToken: &token}
	n.CreatedAt = IDTime(n.ID)
	l, ok := n.Lease()
	if !ok || l.Token != token || l.NotificationID != n.ID || !l.CreatedAt.Equal(n.CreatedAt) {
		t.Fatalf("Lease() = %+v, %v", l, ok)
	}
	n.Status = StatusSent
	if _, ok := n.Lease(); ok {
		t.Error("only a processing notification has a lease")
	}
}

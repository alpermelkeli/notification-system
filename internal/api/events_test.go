package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/httpx"
)

// recordedEvents records published status events.
type recordedEvents struct {
	mu  sync.Mutex
	got []domain.Notification
}

func (r *recordedEvents) Publish(_ context.Context, ns ...domain.Notification) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, ns...)
}

func (r *recordedEvents) statuses() []domain.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.Status, len(r.got))
	for i, n := range r.got {
		out[i] = n.Status
	}
	return out
}

func newServerWithEvents(t *testing.T) (*testServer, *recordedEvents) {
	t.Helper()
	store, evts := newFakeStore(), &recordedEvents{}
	r := chi.NewRouter()
	r.Use(httpx.CorrelationID)
	New(store, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		MaxScheduleAhead: 24 * time.Hour, Now: func() time.Time { return testNow }, Events: evts,
	}).Routes(r)
	return &testServer{store: store, h: r}, evts
}

// TestAPIPublishesItsTransitions: creates, cancels and replays publish the changed rows after the
// store call succeeded; idempotent replays and rejected operations publish nothing (ADR-0011).
func TestAPIPublishesItsTransitions(t *testing.T) {
	s, evts := newServerWithEvents(t)
	body := `{"recipient":"+905551234567","channel":"sms","content":"hi"}`

	var n notificationJSON
	s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "k1").wantStatus(http.StatusAccepted).decode(&n)
	s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "k1").wantStatus(http.StatusAccepted) // replay
	s.do(t, http.MethodPost, "/v1/notifications/"+n.ID.String()+"/cancel", "").wantStatus(http.StatusOK)
	s.do(t, http.MethodPost, "/v1/notifications/"+n.ID.String()+"/cancel", "").wantStatus(http.StatusConflict)
	s.do(t, http.MethodPost, "/v1/notifications/"+n.ID.String()+"/retry", "").wantStatus(http.StatusConflict)

	s.do(t, http.MethodPost, "/v1/notifications/batch", `{"notifications":[
		{"recipient":"+905551234567","channel":"sms","content":"a"},
		{"recipient":"+905551234567","channel":"sms","content":"b"}]}`).wantStatus(http.StatusAccepted)

	want := []domain.Status{domain.StatusPending, domain.StatusCancelled, domain.StatusPending, domain.StatusPending}
	if got := evts.statuses(); !slices.Equal(got, want) {
		t.Fatalf("published %v, want %v", got, want)
	}

	failed := s.store.newNotificationForTest(domain.StatusFailed)
	s.do(t, http.MethodPost, "/v1/notifications/"+failed.String()+"/retry", "").wantStatus(http.StatusAccepted)
	if got := evts.statuses(); len(got) != 5 || got[4] != domain.StatusPending {
		t.Errorf("replay published %v", got)
	}
}

package worker

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/provider"
)

// recordedEvents collects the statuses published per notification, in order.
type recordedEvents struct {
	mu   sync.Mutex
	byID map[uuid.UUID][]domain.Status
}

func (r *recordedEvents) Publish(_ context.Context, ns ...domain.Notification) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byID == nil {
		r.byID = map[uuid.UUID][]domain.Status{}
	}
	for _, n := range ns {
		r.byID[n.ID] = append(r.byID[n.ID], n.Status)
	}
}

func (r *recordedEvents) of(id uuid.UUID) []domain.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.byID[id])
}

// TestWorkerPublishesEveryTransition: the claim and each outcome are published after they are
// persisted (ADR-0011); a stale record changes nothing and publishes nothing.
func TestWorkerPublishesEveryTransition(t *testing.T) {
	evts := &recordedEvents{}
	h := newHarness(t, nil, Options{Events: evts})
	h.sender.fn = func(m provider.Message) (provider.Result, error) {
		switch m.To {
		case "flaky":
			return provider.Result{}, &provider.Error{StatusCode: http.StatusServiceUnavailable}
		case "invalid":
			return provider.Result{}, &provider.Error{StatusCode: http.StatusBadRequest}
		case "throttled":
			return provider.Result{}, &provider.Error{StatusCode: http.StatusTooManyRequests}
		}
		return provider.Result{MessageID: "m"}, nil
	}
	ok := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	flaky := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	invalid := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	throttled := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	cancelled := h.store.add(domain.PriorityNormal, domain.StatusCancelled, 0)
	h.store.mu.Lock()
	h.store.rows[flaky.ID].Recipient = "flaky"
	h.store.rows[invalid.ID].Recipient = "invalid"
	h.store.rows[throttled.ID].Recipient = "throttled"
	h.store.mu.Unlock()
	for _, n := range []domain.Notification{ok, flaky, invalid, throttled, cancelled} {
		h.feed(t, n, 0)
	}
	eventually(t, "all records handled", func() bool { return h.committedAt(normalTopic, 0) == 5 })

	for name, c := range map[string]struct {
		n    domain.Notification
		want []domain.Status
	}{
		"sent":     {ok, []domain.Status{domain.StatusProcessing, domain.StatusSent}},
		"retrying": {flaky, []domain.Status{domain.StatusProcessing, domain.StatusRetrying}},
		"failed":   {invalid, []domain.Status{domain.StatusProcessing, domain.StatusFailed}},
		"released": {throttled, []domain.Status{domain.StatusProcessing, domain.StatusRetrying}},
		"stale":    {cancelled, nil},
	} {
		if got := evts.of(c.n.ID); !slices.Equal(got, c.want) {
			t.Errorf("%s: published %v, want %v", name, got, c.want)
		}
	}
}

// TestUnpersistedOutcomeIsNotPublished: an outcome the store rejected never happened, so no event
// may claim it did.
func TestUnpersistedOutcomeIsNotPublished(t *testing.T) {
	evts := &recordedEvents{}
	h := newHarness(t, nil, Options{Events: evts})
	h.store.outcomeErr = domain.ErrLeaseLost
	n := h.store.add(domain.PriorityNormal, domain.StatusQueued, 0)
	h.feed(t, n, 0)
	eventually(t, "record handled", func() bool { return h.committedAt(normalTopic, 0) == 1 })
	if got := evts.of(n.ID); !slices.Equal(got, []domain.Status{domain.StatusProcessing}) {
		t.Errorf("published %v, want only the claim", got)
	}
}

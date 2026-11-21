package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

func TestRequestHash(t *testing.T) {
	at := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	base := storage.NewNotification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: "+905551234567", Content: "hi", ScheduledAt: &at,
	}
	h := requestHash(canonicalOf(base))

	// Equal requests as the handler sees them: priority omitted vs "normal", and the same instant
	// written with an offset vs in UTC. Both must hash like base.
	s := newServer(t)
	plus3 := `{"recipient":"+905551234567","channel":"sms","content":"hi","scheduledAt":"2026-11-21T18:00:00+03:00"}`
	zulu := `{"recipient":"+905551234567","channel":"sms","content":"hi","priority":"normal","scheduledAt":"2026-11-21T15:00:00Z"}`
	for _, body := range []string{plus3, zulu} {
		s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "same").wantStatus(http.StatusAccepted)
		if got := s.store.lastCreate.IdempotencyKey.RequestHash; got != h {
			t.Errorf("hash of %s differs from the canonical request", body)
		}
	}

	for name, change := range map[string]func(*storage.NewNotification){
		"content":     func(n *storage.NewNotification) { n.Content = "hi!" },
		"recipient":   func(n *storage.NewNotification) { n.Recipient = "+905551234568" },
		"priority":    func(n *storage.NewNotification) { n.Priority = domain.PriorityHigh },
		"channel":     func(n *storage.NewNotification) { n.Channel = domain.ChannelPush },
		"schedule":    func(n *storage.NewNotification) { later := at.Add(time.Microsecond); n.ScheduledAt = &later },
		"no schedule": func(n *storage.NewNotification) { n.ScheduledAt = nil },
	} {
		n := base
		change(&n)
		if requestHash(canonicalOf(n)) == h {
			t.Errorf("changing %s must change the hash", name)
		}
	}
}

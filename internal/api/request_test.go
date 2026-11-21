package api

import (
	"crypto/sha256"
	"net/http"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestRequestHash(t *testing.T) {
	at := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	base := canonical{
		Channel: string(domain.ChannelSMS), Priority: string(domain.PriorityNormal), Recipient: "+905551234567", Content: "hi", ScheduledAt: &at,
	}
	h := requestHash(base)

	// Templates added fields to canonical; they are omitted when empty, so keys stored before
	// templates existed still match their retries.
	if want := sha256.Sum256([]byte("v1\n" + `{"channel":"sms","priority":"normal","recipient":"+905551234567","content":"hi","scheduledAt":"2026-11-21T15:00:00Z"}`)); h != want {
		t.Error("the hash of a plain-content request changed shape")
	}

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

	for name, change := range map[string]func(*canonical){
		"content":     func(n *canonical) { n.Content = "hi!" },
		"recipient":   func(n *canonical) { n.Recipient = "+905551234568" },
		"priority":    func(n *canonical) { n.Priority = string(domain.PriorityHigh) },
		"channel":     func(n *canonical) { n.Channel = string(domain.ChannelPush) },
		"schedule":    func(n *canonical) { later := at.Add(time.Microsecond); n.ScheduledAt = &later },
		"no schedule": func(n *canonical) { n.ScheduledAt = nil },
		"template":    func(n *canonical) { n.Content, n.TemplateID = "", "01a1fd17-4a0b-7869-8fa8-a36f9d5fb75a" },
		"variables":   func(n *canonical) { n.Variables = map[string]string{"code": "1"} },
	} {
		n := base
		change(&n)
		if requestHash(n) == h {
			t.Errorf("changing %s must change the hash", name)
		}
	}
}

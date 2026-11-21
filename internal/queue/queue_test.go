package queue

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestTopics(t *testing.T) {
	if got := Topic(domain.ChannelSMS, domain.PriorityHigh); got != "notifications.sms.high" {
		t.Errorf("Topic = %q", got)
	}
	got := Topics(domain.ChannelEmail)
	want := []string{"notifications.email.high", "notifications.email.normal", "notifications.email.low"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Topics = %v, want %v", got, want)
		}
	}
	if Group(domain.ChannelPush) != "workers.push" {
		t.Errorf("Group = %q", Group(domain.ChannelPush))
	}
	for _, c := range domain.Channels {
		for _, p := range domain.Priorities {
			gc, gp, ok := ParseTopic(Topic(c, p))
			if !ok || gc != c || gp != p {
				t.Errorf("ParseTopic(Topic(%s, %s)) = %s, %s, %v", c, p, gc, gp, ok)
			}
		}
	}
	for _, bad := range []string{"notifications.dlq", "notification.events", "notifications.sms.", "notifications.fax.high", "x.sms.high", "notifications.sms"} {
		if _, _, ok := ParseTopic(bad); ok {
			t.Errorf("ParseTopic(%q) accepted a non-work topic", bad)
		}
	}
}

func TestMessageRoundTrip(t *testing.T) {
	now := time.Now()
	id := domain.NewIDAt(now)
	n := domain.Notification{
		ID: id, CreatedAt: domain.IDTime(id), Channel: domain.ChannelPush, Priority: domain.PriorityLow,
		Attempts: 2, NextAttemptAt: now.Add(time.Second).UTC(),
	}
	m := NewMessage(n)
	if m.Attempt != 3 {
		t.Errorf("Attempt = %d, want the upcoming attempt 3", m.Attempt)
	}
	r, err := m.Record("corr-9")
	if err != nil {
		t.Fatal(err)
	}
	if r.Topic != "notifications.push.low" || string(r.Key) != id.String() || CorrelationID(r) != "corr-9" {
		t.Fatalf("record = topic %s key %s headers %v", r.Topic, r.Key, r.Headers)
	}
	got, err := Decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || !got.CreatedAt.Equal(m.CreatedAt) || !got.DueAt.Equal(m.DueAt) || got != m {
		t.Errorf("Decode = %+v, want %+v", got, m)
	}
	if r, _ := m.Record(""); len(r.Headers) != 0 || CorrelationID(r) != "" {
		t.Errorf("empty correlation id should add no header: %v", r.Headers)
	}
}

func TestDecodeRejectsInconsistentRecords(t *testing.T) {
	id := domain.NewIDAt(time.Now())
	good := Message{ID: id, CreatedAt: domain.IDTime(id), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}
	r, err := good.Record("")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]*kgo.Record{
		"not json":         {Topic: r.Topic, Value: []byte("{")},
		"wrong topic":      {Topic: "notifications.sms.high", Value: r.Value},
		"not a work topic": {Topic: "notifications.dlq", Value: r.Value},
		"zero id":          {Topic: r.Topic, Value: []byte(`{"channel":"sms","priority":"normal","created_at":"2026-01-01T00:00:00Z"}`)},
	}
	for name, rec := range cases {
		if _, err := Decode(rec); err == nil {
			t.Errorf("%s: decoded without error", name)
		}
	}
}

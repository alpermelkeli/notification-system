package queue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestStatusEventRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 4, 5, 123456000, time.UTC)
	id := domain.NewIDAt(now)
	batch := domain.NewIDAt(now)
	cause := "retryable: provider returned 503: invalid recipient +905551234567"
	n := domain.Notification{
		ID: id, CreatedAt: domain.IDTime(id), BatchID: &batch, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
		Recipient: "+905551234567", Content: "secret code 482913", Status: domain.StatusRetrying, Attempts: 2,
		NextAttemptAt: now.Add(4 * time.Second), LastError: &cause, UpdatedAt: now,
	}
	r, err := NewStatusEvent(n).Record("corr-1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Topic != EventsTopic || string(r.Key) != id.String() || CorrelationID(r) != "corr-1" {
		t.Errorf("record = topic %s key %s headers %v", r.Topic, r.Key, r.Headers)
	}
	if v := string(r.Value); strings.Contains(v, "+905551234567") || strings.Contains(v, "482913") {
		t.Errorf("the event carries the recipient or the content: %s", v)
	}
	var fields map[string]any
	if err := json.Unmarshal(r.Value, &fields); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"notificationId", "batchId", "channel", "priority", "status", "attempts", "nextAttemptAt", "lastError", "at"} {
		if _, ok := fields[k]; !ok {
			t.Errorf("field %s missing from %s", k, r.Value)
		}
	}

	e, err := DecodeStatusEvent(r)
	if err != nil {
		t.Fatal(err)
	}
	if e.NotificationID != id || *e.BatchID != batch || e.Status != domain.StatusRetrying || e.Attempts != 2 ||
		!e.At.Equal(now) || !e.NextAttemptAt.Equal(n.NextAttemptAt) || *e.LastError != "retryable: provider returned 503" {
		t.Errorf("decoded %+v", e)
	}
}

func TestStatusEventFieldsByStatus(t *testing.T) {
	cause := "old error"
	base := domain.Notification{ID: domain.NewIDAt(time.Now()), NextAttemptAt: time.Now(), LastError: &cause}
	for status, want := range map[domain.Status]struct{ next, lastErr bool }{
		domain.StatusPending:    {next: true},
		domain.StatusQueued:     {},
		domain.StatusProcessing: {},
		domain.StatusRetrying:   {next: true, lastErr: true},
		domain.StatusSent:       {},
		domain.StatusFailed:     {lastErr: true},
		domain.StatusCancelled:  {},
	} {
		n := base
		n.Status = status
		e := NewStatusEvent(n)
		if (e.NextAttemptAt != nil) != want.next || (e.LastError != nil) != want.lastErr {
			t.Errorf("%s: nextAttemptAt=%v lastError=%v", status, e.NextAttemptAt, e.LastError)
		}
	}
}

func TestDecodeStatusEventRejectsGarbage(t *testing.T) {
	for _, v := range []string{"not json", `{}`, `{"notificationId":"01a1fd17-4a0b-7869-8fa8-a36f9d5fb75a"}`} {
		if _, err := DecodeStatusEvent(&kgo.Record{Value: []byte(v)}); err == nil {
			t.Errorf("%s decoded without error", v)
		}
	}
}

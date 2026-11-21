package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestTemplateLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tpl, err := s.CreateTemplate(ctx, TemplateInput{Name: "order_shipped", Channel: domain.ChannelSMS, Body: "Order {{id}} shipped"})
	if err != nil {
		t.Fatal(err)
	}
	if tpl.Version != 1 || !domain.IsV7(tpl.ID) || tpl.Channel != domain.ChannelSMS || tpl.CreatedAt.IsZero() {
		t.Errorf("unexpected new template: %+v", tpl)
	}
	if _, err := s.CreateTemplate(ctx, TemplateInput{Name: "order_shipped", Channel: domain.ChannelEmail, Body: "x"}); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Errorf("duplicate name: got %v, want ErrAlreadyExists", err)
	}

	got, err := s.GetTemplate(ctx, tpl.ID)
	if err != nil || got != tpl {
		t.Errorf("get: %+v, %v; want %+v", got, err, tpl)
	}

	upd, err := s.UpdateTemplate(ctx, tpl.ID, TemplateInput{Name: "order_shipped_v2", Channel: domain.ChannelSMS, Body: "Order {{id}} is on its way"})
	if err != nil {
		t.Fatal(err)
	}
	if upd.Version != 2 || upd.Name != "order_shipped_v2" || upd.Body != "Order {{id}} is on its way" || !upd.CreatedAt.Equal(tpl.CreatedAt) {
		t.Errorf("unexpected update: %+v", upd)
	}
	if _, err := s.UpdateTemplate(ctx, tpl.ID, TemplateInput{Name: "x", Channel: domain.ChannelPush, Body: "x"}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("channel change: got %v, want ErrInvalidArgument", err)
	}
	other, err := s.CreateTemplate(ctx, TemplateInput{Name: "other", Channel: domain.ChannelSMS, Body: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateTemplate(ctx, other.ID, TemplateInput{Name: "order_shipped_v2", Channel: domain.ChannelSMS, Body: "x"}); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Errorf("rename onto a taken name: got %v, want ErrAlreadyExists", err)
	}

	if err := s.DeleteTemplate(ctx, tpl.ID); err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"get": func() error { _, err := s.GetTemplate(ctx, tpl.ID); return err }(),
		"update": func() error {
			_, err := s.UpdateTemplate(ctx, tpl.ID, TemplateInput{Name: "y", Channel: domain.ChannelSMS, Body: "y"})
			return err
		}(),
		"delete": s.DeleteTemplate(ctx, tpl.ID),
	} {
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("%s after delete: got %v, want ErrNotFound", name, err)
		}
	}
}

func TestListAndGetTemplates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var ids []uuid.UUID
	for i := range 5 {
		ch := domain.ChannelSMS
		if i%2 == 1 {
			ch = domain.ChannelEmail
		}
		tpl, err := s.CreateTemplate(ctx, TemplateInput{Name: fmt.Sprintf("t%d", i), Channel: ch, Body: "x"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, tpl.ID)
	}

	// Newest first, two per page.
	var seen []uuid.UUID
	f := TemplateFilter{Limit: 2}
	for {
		page, err := s.ListTemplates(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range page.Items {
			seen = append(seen, it.ID)
		}
		if page.Next == nil {
			break
		}
		f.After = page.Next
	}
	// Ids created within one millisecond have no defined order, so compare with the ids sorted.
	want := slices.Clone(ids)
	slices.SortFunc(want, func(a, b uuid.UUID) int { return bytes.Compare(b[:], a[:]) })
	if !slices.Equal(seen, want) {
		t.Errorf("pages = %v, want every template once in descending id order %v", seen, want)
	}

	email := domain.ChannelEmail
	page, err := s.ListTemplates(ctx, TemplateFilter{Channel: &email})
	if err != nil || len(page.Items) != 2 || page.Next != nil {
		t.Errorf("email filter: %+v, %v", page, err)
	}

	got, err := s.GetTemplates(ctx, []uuid.UUID{ids[0], ids[3], domain.NewIDAt(time.Now())})
	if err != nil || len(got) != 2 || got[ids[3]].Name != "t3" {
		t.Errorf("GetTemplates = %+v, %v", got, err)
	}
}

func TestCreateNotificationRecordsItsTemplate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	ref := &TemplateRef{ID: domain.NewIDAt(time.Now()), Version: 3}
	in := sms("rendered text")
	in.Template = ref
	n, _, err := s.CreateNotification(ctx, in, Origin{})
	if err != nil {
		t.Fatal(err)
	}
	if n.TemplateID == nil || *n.TemplateID != ref.ID || n.TemplateVersion == nil || *n.TemplateVersion != 3 {
		t.Errorf("template ref = %v/%v, want %+v", n.TemplateID, n.TemplateVersion, ref)
	}

	res, err := s.CreateBatch(ctx, NewBatch{Items: []NewNotification{in, sms("plain")}}, Origin{})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Items[0].Notification; got.TemplateID == nil || *got.TemplateVersion != 3 {
		t.Errorf("batch item 0 lost its template: %+v", got)
	}
	if got := res.Items[1].Notification; got.TemplateID != nil || got.TemplateVersion != nil {
		t.Errorf("batch item 1 has a template: %+v", got)
	}
}

func TestFindIdempotencyKey(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	in := sms("x")
	in.IdempotencyKey = key("single", "a")
	n, _, err := s.CreateNotification(ctx, in, Origin{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.CreateBatch(ctx, NewBatch{Items: []NewNotification{sms("y")}, IdempotencyKey: key("batch", "b")}, Origin{})
	if err != nil {
		t.Fatal(err)
	}

	if m, err := s.FindIdempotencyKey(ctx, *key("single", "a")); err != nil || !m.Found || m.Batch || m.ID != n.ID {
		t.Errorf("single: %+v, %v", m, err)
	}
	if m, err := s.FindIdempotencyKey(ctx, *key("batch", "b")); err != nil || !m.Found || !m.Batch || m.ID != res.Batch.ID {
		t.Errorf("batch: %+v, %v", m, err)
	}
	if _, err := s.FindIdempotencyKey(ctx, *key("single", "other body")); !errors.Is(err, domain.ErrIdempotencyKeyReused) {
		t.Errorf("different hash: %v", err)
	}
	if m, err := s.FindIdempotencyKey(ctx, *key("unknown", "a")); err != nil || m.Found {
		t.Errorf("unknown key: %+v, %v", m, err)
	}
	// An expired key is not live.
	if _, err := testPool.Exec(ctx, `UPDATE idempotency_keys SET expires_at = now() - interval '1 second' WHERE key = 'single'`); err != nil {
		t.Fatal(err)
	}
	if m, err := s.FindIdempotencyKey(ctx, *key("single", "a")); err != nil || m.Found {
		t.Errorf("expired key: %+v, %v", m, err)
	}

	replay, err := s.ReplayBatch(ctx, res.Batch.ID, []NewNotification{{}})
	if err != nil || replay.Created || replay.Batch.ID != res.Batch.ID || len(replay.Items) != 1 ||
		replay.Items[0].Notification.ID != res.Items[0].Notification.ID {
		t.Errorf("replay = %+v, %v", replay, err)
	}
}

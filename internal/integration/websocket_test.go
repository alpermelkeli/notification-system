package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
	"github.com/alpermelkeli/notification-system/internal/ws"
)

// TestWebSocketFollowsABatchToDelivery wires the API instance's side of B-4 to the real engine:
// the hub reads notification.events from Kafka without a group, a client subscribes to a batch
// over a real WebSocket, and sees the snapshot and then every item reach sent (ADR-0011).
func TestWebSocketFollowsABatchToDelivery(t *testing.T) {
	if pool == nil {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.New(pool, storage.Options{})
	fp := newFakeProvider()
	provider := httptest.NewServer(fp)
	defer provider.Close()

	hub := ws.NewHub(nil, quiet)
	consumer, err := kgo.NewClient(append(ws.ConsumerOptions(), kgo.SeedBrokers(kafkaAddr))...)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	consumed := make(chan struct{})
	go func() { hub.Consume(ctx, consumer); close(consumed) }()
	defer func() { cancel(); <-consumed }()
	srv := httptest.NewServer(ws.NewHandler(hub, store, quiet, ws.HandlerOptions{}))
	defer srv.Close()
	defer hub.Shutdown()

	// Created before the engine runs, so the snapshot shows them pending.
	res, err := store.CreateBatch(ctx, storage.NewBatch{Items: []storage.NewNotification{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: "+905553330001", Content: "a"},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: "+905553330002", Content: "b"},
	}}, storage.Origin{})
	if err != nil {
		t.Fatal(err)
	}

	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	conn, _, err := websocket.Dial(dctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()
	if err := conn.Write(dctx, websocket.MessageText,
		[]byte(fmt.Sprintf(`{"type":"subscribe","batchIds":[%q]}`, res.Batch.ID))); err != nil {
		t.Fatal(err)
	}

	type message struct {
		Type  string `json:"type"`
		Batch struct {
			StatusCounts map[string]int `json:"statusCounts"`
		} `json:"batch"`
		Event struct {
			NotificationID string `json:"notificationId"`
			Status         string `json:"status"`
		} `json:"event"`
	}
	read := func() message {
		t.Helper()
		rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
		defer rcancel()
		_, data, err := conn.Read(rctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var m message
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if m := read(); m.Type != "subscribed" {
		t.Fatalf("first message %+v, want subscribed", m)
	}
	if m := read(); m.Type != "batch" || m.Batch.StatusCounts["pending"] != 2 {
		t.Fatalf("snapshot %+v, want 2 pending", m)
	}

	startEngine(t, store, provider.URL, 1, 100)
	sent := map[string]bool{}
	for len(sent) < 2 {
		m := read()
		if m.Type == "status" && m.Event.Status == string(domain.StatusSent) {
			sent[m.Event.NotificationID] = true
		}
	}
	for _, it := range res.Items {
		if !sent[it.Notification.ID.String()] {
			t.Errorf("no sent event for %s", it.Notification.ID)
		}
	}
}

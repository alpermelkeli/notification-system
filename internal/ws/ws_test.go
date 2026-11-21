package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/queue"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeStore struct {
	mu            sync.Mutex
	notifications map[uuid.UUID]domain.Notification
	batches       map[uuid.UUID]domain.Batch
}

func (f *fakeStore) GetNotifications(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[uuid.UUID]domain.Notification{}
	for _, id := range ids {
		if n, ok := f.notifications[id]; ok {
			out[id] = n
		}
	}
	return out, nil
}

func (f *fakeStore) GetBatch(_ context.Context, id uuid.UUID) (domain.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.batches[id]
	if !ok {
		return domain.Batch{}, domain.ErrNotFound
	}
	return b, nil
}

type fixture struct {
	hub   *Hub
	store *fakeStore
	srv   *httptest.Server
	reg   *prometheus.Registry
}

// waitDisconnects waits until one disconnect for reason is counted; the handler counts it after
// the close handshake.
func (f *fixture) waitDisconnects(t *testing.T, reason string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for f.disconnects(t, reason) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("no %s disconnect counted", reason)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// disconnects returns notifier_ws_disconnects_total for reason.
func (f *fixture) disconnects(t *testing.T, reason string) float64 {
	t.Helper()
	mfs, err := f.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "notifier_ws_disconnects_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	reg := metrics.NewRegistry()
	f := &fixture{hub: NewHub(metrics.NewWebSocket(reg), discard), store: &fakeStore{
		notifications: map[uuid.UUID]domain.Notification{}, batches: map[uuid.UUID]domain.Batch{},
	}, reg: reg}
	f.srv = httptest.NewServer(NewHandler(f.hub, f.store, discard, HandlerOptions{AllowedOrigins: []string{"*.allowed.example"}}))
	t.Cleanup(func() {
		f.hub.Shutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.hub.Wait(ctx); err != nil {
			t.Errorf("connections did not end after shutdown: %v", err)
		}
		f.srv.Close()
	})
	return f
}

// notification stores a notification and returns it.
func (f *fixture) notification(status domain.Status, batch *uuid.UUID) domain.Notification {
	now := time.Now().UTC().Truncate(time.Microsecond)
	n := domain.Notification{
		ID: domain.NewIDAt(now), BatchID: batch, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
		Status: status, UpdatedAt: now, NextAttemptAt: now,
	}
	f.store.mu.Lock()
	f.store.notifications[n.ID] = n
	f.store.mu.Unlock()
	return n
}

type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
}

func (f *fixture) dial(t *testing.T) *wsClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(f.srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetReadLimit(1 << 20)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &wsClient{t: t, conn: conn}
}

func (c *wsClient) send(msg string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
		c.t.Fatal(err)
	}
}

// next reads one message into a generic map.
func (c *wsClient) next() map[string]any {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := c.conn.Read(ctx)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		c.t.Fatal(err)
	}
	return m
}

// expect reads one message and checks its type.
func (c *wsClient) expect(typ string) map[string]any {
	c.t.Helper()
	m := c.next()
	if m["type"] != typ {
		c.t.Fatalf("got %v, want a %s message", m, typ)
	}
	return m
}

// quiet checks that nothing arrives for a short while.
func (c *wsClient) quiet() {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, data, err := c.conn.Read(ctx); err == nil {
		c.t.Fatalf("unexpected message %s", data)
	}
}

func event(n domain.Notification, status domain.Status) queue.StatusEvent {
	n.Status, n.UpdatedAt = status, time.Now().UTC()
	return queue.NewStatusEvent(n)
}

func eventStatus(m map[string]any) string {
	e, _ := m["event"].(map[string]any)
	s, _ := e["status"].(string)
	return s
}

func TestSubscribeSnapshotAndEvents(t *testing.T) {
	f := newFixture(t)
	n := f.notification(domain.StatusQueued, nil)
	other := f.notification(domain.StatusQueued, nil)
	c := f.dial(t)

	c.send(fmt.Sprintf(`{"type":"subscribe","notificationIds":[%q]}`, n.ID))
	ack := c.expect(typeSubscribed)
	if ids, _ := ack["notificationIds"].([]any); len(ids) != 1 || ids[0] != n.ID.String() {
		t.Errorf("ack = %v", ack)
	}
	if b, ok := ack["batchIds"].([]any); !ok || len(b) != 0 {
		t.Errorf("ack must list batchIds as an empty array: %v", ack)
	}
	snap := c.expect(typeStatus)
	if snap["snapshot"] != true || eventStatus(snap) != "queued" {
		t.Errorf("snapshot = %v", snap)
	}

	f.hub.Publish(event(other, domain.StatusSent)) // not subscribed
	f.hub.Publish(event(n, domain.StatusProcessing))
	live := c.expect(typeStatus)
	if live["snapshot"] != nil || eventStatus(live) != "processing" {
		t.Errorf("live event = %v", live)
	}
	e, _ := live["event"].(map[string]any)
	if e["notificationId"] != n.ID.String() || e["channel"] != "sms" {
		t.Errorf("event fields = %v", e)
	}

	c.send(fmt.Sprintf(`{"type":"unsubscribe","notificationIds":[%q]}`, n.ID))
	if ids, _ := c.expect(typeSubscribed)["notificationIds"].([]any); len(ids) != 0 {
		t.Errorf("still subscribed after unsubscribe: %v", ids)
	}
	f.hub.Publish(event(n, domain.StatusSent))
	c.quiet()
}

func TestBatchSubscription(t *testing.T) {
	f := newFixture(t)
	batch := domain.NewIDAt(time.Now())
	f.store.batches[batch] = domain.Batch{ID: batch, Total: 2, StatusCounts: map[domain.Status]int{domain.StatusPending: 2}}
	a, b := f.notification(domain.StatusPending, &batch), f.notification(domain.StatusPending, &batch)
	c := f.dial(t)

	// Subscribed to the batch and to one of its notifications: that notification's events arrive
	// once, not twice.
	c.send(fmt.Sprintf(`{"type":"subscribe","batchIds":[%q],"notificationIds":[%q]}`, batch, a.ID))
	c.expect(typeSubscribed)
	c.expect(typeStatus) // snapshot of a
	snap := c.expect(typeBatch)
	bs, _ := snap["batch"].(map[string]any)
	if counts, _ := bs["statusCounts"].(map[string]any); bs["total"] != float64(2) || counts["pending"] != float64(2) {
		t.Errorf("batch snapshot = %v", snap)
	}

	f.hub.Publish(event(a, domain.StatusQueued))
	f.hub.Publish(event(b, domain.StatusQueued))
	got := map[string]int{}
	for range 2 {
		e, _ := c.expect(typeStatus)["event"].(map[string]any)
		got[e["notificationId"].(string)]++
	}
	if got[a.ID.String()] != 1 || got[b.ID.String()] != 1 {
		t.Errorf("events per notification = %v", got)
	}
	c.quiet()
}

func TestProtocolErrors(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	unknown := domain.NewIDAt(time.Now())

	for _, tc := range []struct{ msg, code string }{
		{`not json`, "bad_request"},
		{`{"type":"subscribe","notificationIds":["x"],"extra":1}`, "bad_request"},
		{`{"type":"listen","notificationIds":["` + unknown.String() + `"]}`, "bad_request"},
		{`{"type":"subscribe"}`, "bad_request"},
		{`{"type":"subscribe","notificationIds":["nope"]}`, "invalid_id"},
	} {
		c.send(tc.msg)
		if m := c.expect(typeError); m["code"] != tc.code {
			t.Errorf("%s: code %v, want %s", tc.msg, m["code"], tc.code)
		}
	}

	ids := make([]string, MaxIDsPerMessage+1)
	for i := range ids {
		ids[i] = `"` + domain.NewIDAt(time.Now()).String() + `"`
	}
	c.send(`{"type":"subscribe","notificationIds":[` + strings.Join(ids, ",") + `]}`)
	if m := c.expect(typeError); m["code"] != "bad_request" {
		t.Errorf("too many ids in one message: %v", m)
	}

	// An unknown id is reported and not kept as a subscription.
	c.send(fmt.Sprintf(`{"type":"subscribe","notificationIds":[%q]}`, unknown))
	c.expect(typeSubscribed)
	if m := c.expect(typeError); m["code"] != "not_found" || m["id"] != unknown.String() {
		t.Errorf("unknown id: %v", m)
	}
	c.send(fmt.Sprintf(`{"type":"unsubscribe","notificationIds":[%q]}`, unknown))
	if ids, _ := c.expect(typeSubscribed)["notificationIds"].([]any); len(ids) != 0 {
		t.Errorf("unknown id kept: %v", ids)
	}

	// The connection survived every error.
	n := f.notification(domain.StatusPending, nil)
	c.send(fmt.Sprintf(`{"type":"subscribe","notificationIds":[%q]}`, n.ID))
	c.expect(typeSubscribed)
	c.expect(typeStatus)
}

func TestSubscriptionLimit(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	for i := 0; i < MaxSubscriptions; i += MaxIDsPerMessage {
		ids := make([]string, MaxIDsPerMessage)
		for j := range ids {
			n := f.notification(domain.StatusPending, nil)
			ids[j] = `"` + n.ID.String() + `"`
		}
		c.send(`{"type":"subscribe","notificationIds":[` + strings.Join(ids, ",") + `]}`)
		c.expect(typeSubscribed)
		for range ids {
			c.expect(typeStatus)
		}
	}
	extra := f.notification(domain.StatusPending, nil)
	c.send(fmt.Sprintf(`{"type":"subscribe","notificationIds":[%q]}`, extra.ID))
	if m := c.expect(typeError); m["code"] != "too_many_subscriptions" {
		t.Errorf("subscription %d: %v", MaxSubscriptions+1, m)
	}
}

func TestSlowConsumerIsDisconnected(t *testing.T) {
	f := newFixture(t)
	n := f.notification(domain.StatusPending, nil)
	c := f.dial(t)
	c.send(fmt.Sprintf(`{"type":"subscribe","notificationIds":[%q]}`, n.ID))
	c.expect(typeSubscribed)
	c.expect(typeStatus)

	// Without reading, the socket buffers fill up, then the send queue overflows.
	for range 200_000 {
		f.hub.Publish(event(n, domain.StatusProcessing))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		_, _, err := c.conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Fatalf("closed with %v, want 1008", err)
			}
			break
		}
	}
	f.waitDisconnects(t, metrics.WSSlowConsumer)
}

func TestShutdownClosesConnectionsAndRefusesNewOnes(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	f.hub.Shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := c.conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("got %v, want close 1001", err)
	}
	late := f.dial(t)
	if _, _, err := late.conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("connection after shutdown: %v, want close 1001", err)
	}
}

func TestBinaryMessagesAreRejected(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageBinary, []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusUnsupportedData {
		t.Fatalf("got %v, want close 1003", err)
	}
}

func TestCrossOriginBrowsersAreRejected(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(f.srv.URL, "http"), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}},
	})
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin dial: err %v, resp %v; want 403", err, resp)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func TestAllowedOriginsMayConnect(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(f.srv.URL, "http"), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://app.allowed.example"}},
	})
	if err != nil {
		t.Fatalf("an allowed origin was refused: %v", err)
	}
	_ = conn.CloseNow()
}

func TestClientCloseIsCounted(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	if err := c.conn.Close(websocket.StatusNormalClosure, "bye"); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f.waitDisconnects(t, metrics.WSClosedByClient)
}

// TestRepliesDoNotCrowdOutEvents: a subscribe of 100 ids queues 101 replies; events have a queue
// of their own, so events arriving meanwhile neither get dropped nor disconnect the client.
func TestRepliesDoNotCrowdOutEvents(t *testing.T) {
	f := newFixture(t)
	ids := make([]string, MaxIDsPerMessage)
	var first domain.Notification
	for i := range ids {
		n := f.notification(domain.StatusPending, nil)
		if i == 0 {
			first = n
		}
		ids[i] = `"` + n.ID.String() + `"`
	}
	c := f.dial(t)
	c.send(`{"type":"subscribe","notificationIds":[` + strings.Join(ids, ",") + `]}`)
	time.Sleep(200 * time.Millisecond) // the subscription is in place; nothing has been read yet
	for range sendQueue - 10 {
		f.hub.Publish(event(first, domain.StatusQueued))
	}
	statuses := 0
	for statuses < MaxIDsPerMessage+sendQueue-10 {
		if m := c.next(); m["type"] == typeStatus {
			statuses++
		}
	}
	if got := f.disconnects(t, metrics.WSSlowConsumer); got != 0 {
		t.Errorf("slow consumer disconnects = %v", got)
	}
}

// TestRequestsAreRateLimited: requests are handled at most requestRate per second; a burst beyond
// the pending queue is answered with rate_limited instead of blocking the reader.
func TestRequestsAreRateLimited(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	n := f.notification(domain.StatusPending, nil)
	msg := fmt.Sprintf(`{"type":"unsubscribe","notificationIds":[%q]}`, n.ID)
	const burst = 40
	for range burst {
		c.send(msg)
	}
	limited, acks := 0, 0
	for limited+acks < burst {
		switch m := c.next(); m["type"] {
		case typeError:
			if m["code"] == "rate_limited" {
				limited++
			}
		case typeSubscribed:
			acks++
		}
	}
	if limited == 0 || acks < pendingRequests {
		t.Errorf("%d answered, %d rate limited; want both", acks, limited)
	}
}

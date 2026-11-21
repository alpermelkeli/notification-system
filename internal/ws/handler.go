package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/queue"
)

// Connection timing and size limits (ADR-0011).
const (
	maxMessageBytes = 64 << 10
	pingInterval    = 30 * time.Second
	writeTimeout    = 10 * time.Second
	snapshotTimeout = 5 * time.Second
)

// Store reads the current state sent as a snapshot after a subscribe. *storage.Store implements
// it.
type Store interface {
	GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	GetBatch(ctx context.Context, id uuid.UUID) (domain.Batch, error)
}

// Message types. Clients send subscribe and unsubscribe; the server sends the others.
const (
	typeSubscribe   = "subscribe"
	typeUnsubscribe = "unsubscribe"
	typeSubscribed  = "subscribed"
	typeStatus      = "status"
	typeBatch       = "batch"
	typeError       = "error"
)

// clientMessage is a subscribe or unsubscribe request.
type clientMessage struct {
	Type            string   `json:"type"`
	NotificationIDs []string `json:"notificationIds"`
	BatchIDs        []string `json:"batchIds"`
}

// subscribedMessage acknowledges a subscribe or unsubscribe with the full current set.
type subscribedMessage struct {
	Type            string      `json:"type"`
	NotificationIDs []uuid.UUID `json:"notificationIds"`
	BatchIDs        []uuid.UUID `json:"batchIds"`
}

// statusMessage carries a status event, or the current state right after a subscribe (snapshot).
type statusMessage struct {
	Type     string            `json:"type"`
	Event    queue.StatusEvent `json:"event"`
	Snapshot bool              `json:"snapshot,omitempty"`
}

// batchMessage carries a batch's status counts right after a subscribe.
type batchMessage struct {
	Type  string        `json:"type"`
	Batch batchSnapshot `json:"batch"`
}

type batchSnapshot struct {
	ID           uuid.UUID      `json:"id"`
	CreatedAt    time.Time      `json:"createdAt"`
	Total        int            `json:"total"`
	StatusCounts map[string]int `json:"statusCounts"`
}

type errorMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
	ID      string `json:"id,omitempty"`
}

// Handler serves GET /v1/ws.
type Handler struct {
	hub     *Hub
	store   Store
	logger  *slog.Logger
	metrics *metrics.WebSocket
}

// NewHandler returns the WebSocket endpoint of hub. Snapshots are read from store.
func NewHandler(hub *Hub, store Store, logger *slog.Logger) *Handler {
	return &Handler{hub: hub, store: store, logger: logger, metrics: hub.metrics}
}

// ServeHTTP upgrades the request and serves the connection until either side closes it.
// Browsers may only connect from the API's own origin (the library's default check); clients
// without an Origin header, such as CLI tools and services, are accepted.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has answered the request
	}
	// The request context must not be used after the upgrade; keep its values (correlation id).
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()

	c := newClient()
	if !h.hub.register(c) {
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	h.metrics.Connected()
	conn.SetReadLimit(maxMessageBytes)

	var readErr error
	readDone := make(chan struct{})
	go func() { defer close(readDone); readErr = h.read(ctx, conn, c) }()

	reason := h.write(ctx, conn, c, readDone, func() error { return readErr })
	// Stop the reader before unregistering, so it cannot add subscriptions for a gone client.
	cancel()
	_ = conn.CloseNow()
	<-readDone
	h.hub.unregister(c)
	h.metrics.Disconnected(reason)
	h.logger.DebugContext(ctx, "websocket closed", "reason", reason)
}

// write sends queued messages and pings until the connection ends, closes it with the right code
// and returns why it ended. readErr may be called once readDone is closed.
func (h *Handler) write(ctx context.Context, conn *websocket.Conn, c *client, readDone <-chan struct{}, readErr func() error) string {
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case msg := <-c.out:
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				_ = conn.CloseNow()
				return metrics.WSError
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				_ = conn.CloseNow()
				return metrics.WSPingTimeout
			}
		case <-c.slow:
			_ = conn.Close(websocket.StatusPolicyViolation, "client too slow; reconnect and subscribe again")
			return metrics.WSSlowConsumer
		case <-h.hub.done:
			_ = conn.Close(websocket.StatusGoingAway, "server shutting down; reconnect")
			return metrics.WSShutdown
		case <-readDone:
			err := readErr()
			switch status := websocket.CloseStatus(err); status {
			case websocket.StatusUnsupportedData, websocket.StatusMessageTooBig:
				_ = conn.Close(status, err.Error())
				return metrics.WSError
			default:
				// A close frame from the client, or a dropped connection.
				return metrics.WSClosedByClient
			}
		}
	}
}

// read handles client messages until the connection fails or the client closes it.
func (h *Handler) read(ctx context.Context, conn *websocket.Conn, c *client) error {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if typ != websocket.MessageText {
			return websocket.CloseError{Code: websocket.StatusUnsupportedData, Reason: "only JSON text messages are accepted"}
		}
		if err := h.handle(ctx, c, data); err != nil {
			return err
		}
	}
}

// handle processes one client message. Problems with the message are answered with an error
// message and the connection stays open; only a closed connection ends it.
func (h *Handler) handle(ctx context.Context, c *client, data []byte) error {
	var m clientMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return h.reply(ctx, c, errorMessage{Type: typeError, Code: "bad_request", Message: "malformed message: " + err.Error()})
	}
	if m.Type != typeSubscribe && m.Type != typeUnsubscribe {
		return h.reply(ctx, c, errorMessage{Type: typeError, Code: "bad_request",
			Message: fmt.Sprintf("unknown type %q; send subscribe or unsubscribe", m.Type)})
	}
	if n := len(m.NotificationIDs) + len(m.BatchIDs); n == 0 || n > MaxIDsPerMessage {
		return h.reply(ctx, c, errorMessage{Type: typeError, Code: "bad_request",
			Message: fmt.Sprintf("a message must name 1 to %d notificationIds and batchIds, got %d", MaxIDsPerMessage, n)})
	}
	notifications, bad := parseIDs(m.NotificationIDs)
	batches, badBatch := parseIDs(m.BatchIDs)
	if bad == "" {
		bad = badBatch
	}
	if bad != "" {
		return h.reply(ctx, c, errorMessage{Type: typeError, Code: "invalid_id", Message: "not a UUID", ID: bad})
	}

	if m.Type == typeUnsubscribe {
		h.hub.unsubscribe(c, notifications, batches)
		return h.replySubscriptions(ctx, c)
	}
	newNotifications, newBatches, err := h.hub.subscribe(c, notifications, batches)
	if err != nil {
		return h.reply(ctx, c, errorMessage{Type: typeError, Code: "too_many_subscriptions",
			Message: fmt.Sprintf("a connection may follow at most %d ids; unsubscribe first", MaxSubscriptions)})
	}
	if err := h.replySubscriptions(ctx, c); err != nil {
		return err
	}
	// The subscription is in place before the snapshot is read, so no change in between is lost;
	// an event may still overtake its snapshot, and clients keep the latest "at".
	return h.snapshot(ctx, c, newNotifications, newBatches)
}

// snapshot sends the current state of newly subscribed ids. An id that does not exist is
// unsubscribed again and reported.
func (h *Handler) snapshot(ctx context.Context, c *client, notifications, batches []uuid.UUID) error {
	for _, id := range notifications {
		sctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
		n, err := h.store.GetNotification(sctx, id)
		cancel()
		var msg any
		switch {
		case errors.Is(err, domain.ErrNotFound):
			h.hub.unsubscribe(c, []uuid.UUID{id}, nil)
			msg = errorMessage{Type: typeError, Code: "not_found", Message: "no notification with this id", ID: id.String()}
		case err != nil:
			h.logger.WarnContext(ctx, "websocket snapshot failed", "notification_id", id, "err", err)
			msg = errorMessage{Type: typeError, Code: "snapshot_failed", Message: "current status unavailable; updates follow", ID: id.String()}
		default:
			msg = statusMessage{Type: typeStatus, Event: queue.NewStatusEvent(n), Snapshot: true}
		}
		if err := h.reply(ctx, c, msg); err != nil {
			return err
		}
	}
	for _, id := range batches {
		sctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
		b, err := h.store.GetBatch(sctx, id)
		cancel()
		var msg any
		switch {
		case errors.Is(err, domain.ErrNotFound):
			h.hub.unsubscribe(c, nil, []uuid.UUID{id})
			msg = errorMessage{Type: typeError, Code: "not_found", Message: "no batch with this id", ID: id.String()}
		case err != nil:
			h.logger.WarnContext(ctx, "websocket snapshot failed", "batch_id", id, "err", err)
			msg = errorMessage{Type: typeError, Code: "snapshot_failed", Message: "current counts unavailable; updates follow", ID: id.String()}
		default:
			counts := make(map[string]int, len(b.StatusCounts))
			for s, n := range b.StatusCounts {
				counts[string(s)] = n
			}
			msg = batchMessage{Type: typeBatch, Batch: batchSnapshot{ID: b.ID, CreatedAt: b.CreatedAt, Total: b.Total, StatusCounts: counts}}
		}
		if err := h.reply(ctx, c, msg); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) replySubscriptions(ctx context.Context, c *client) error {
	notifications, batches := h.hub.subscriptions(c)
	return h.reply(ctx, c, subscribedMessage{Type: typeSubscribed, NotificationIDs: notifications, BatchIDs: batches})
}

// reply queues an answer to the client's own request. Unlike events it waits for room in the
// queue: a client that sends many requests is slowed down, not disconnected.
func (h *Handler) reply(ctx context.Context, c *client, msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode websocket reply: %w", err)
	}
	select {
	case c.out <- data:
		h.metrics.Sent(messageType(msg))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func messageType(msg any) string {
	switch m := msg.(type) {
	case statusMessage:
		return m.Type
	case batchMessage:
		return m.Type
	case subscribedMessage:
		return m.Type
	case errorMessage:
		return m.Type
	}
	return "unknown"
}

// parseIDs parses UUIDs and returns the first one that is not valid, if any.
func parseIDs(ss []string) ([]uuid.UUID, string) {
	out := make([]uuid.UUID, 0, len(ss))
	for _, s := range ss {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, s
		}
		out = append(out, id)
	}
	return out, ""
}

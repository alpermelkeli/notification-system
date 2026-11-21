package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
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
	// requestRate bounds the client messages handled per second and connection; pendingRequests
	// may wait, and further ones are answered with rate_limited.
	requestRate     = 10
	pendingRequests = 8
)

// Store reads the current state sent as a snapshot after a subscribe. *storage.Store implements
// it.
type Store interface {
	// GetNotifications returns the notifications that exist among ids, by id.
	GetNotifications(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]domain.Notification, error)
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
	accept  *websocket.AcceptOptions
}

// HandlerOptions configures a Handler.
type HandlerOptions struct {
	// AllowedOrigins are browser origins allowed besides the API's own, as path.Match patterns of
	// the host ("*.example.com") or of scheme and host ("https://app.example.com"). Needed when a
	// proxy in front of the API changes the Host header, or a web app on another domain connects.
	AllowedOrigins []string
}

// NewHandler returns the WebSocket endpoint of hub. Snapshots are read from store.
func NewHandler(hub *Hub, store Store, logger *slog.Logger, opts HandlerOptions) *Handler {
	return &Handler{
		hub: hub, store: store, logger: logger, metrics: hub.metrics,
		accept: &websocket.AcceptOptions{OriginPatterns: opts.AllowedOrigins},
	}
}

// ServeHTTP upgrades the request and serves the connection until either side closes it.
// Browsers may connect from the API's own origin and from AllowedOrigins; clients without an
// Origin header, such as CLI tools and services, are accepted.
//
// Four goroutines serve a connection. The reader only reads, so pongs are always processed; the
// processor handles requests one at a time, at most requestRate per second; the pinger checks
// the peer; this goroutine writes and decides when the connection ends.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, h.accept)
	if err != nil {
		return // Accept has answered the request
	}
	// The request context must not be used after the upgrade; keep its values (correlation id).
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()

	c := newClient()
	switch err := h.hub.register(c); {
	case errors.Is(err, errShuttingDown):
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	case errors.Is(err, errTooManyConnections):
		_ = conn.Close(websocket.StatusTryAgainLater, "too many connections; try again later")
		return
	}
	h.metrics.Connected()
	conn.SetReadLimit(maxMessageBytes)

	var (
		wg      sync.WaitGroup
		readErr error
	)
	readDone, pingFailed := make(chan struct{}), make(chan struct{})
	requests := make(chan []byte, pendingRequests)
	wg.Go(func() { defer close(readDone); readErr = h.read(ctx, conn, c, requests) })
	wg.Go(func() { h.process(ctx, c, requests) })
	wg.Go(func() { h.ping(ctx, conn, pingFailed) })

	reason := h.write(ctx, conn, c, readDone, pingFailed, func() error { return readErr })
	// Stop the other goroutines before unregistering, so none can add subscriptions for a gone
	// client.
	cancel()
	_ = conn.CloseNow()
	wg.Wait()
	h.hub.unregister(c)
	h.metrics.Disconnected(reason)
	h.logger.DebugContext(ctx, "websocket closed", "reason", reason)
}

// write sends replies and events until the connection ends, closes it with the right code and
// returns why it ended. readErr may be called once readDone is closed.
func (h *Handler) write(ctx context.Context, conn *websocket.Conn, c *client, readDone, pingFailed <-chan struct{}, readErr func() error) string {
	send := func(msg []byte) bool {
		wctx, cancel := context.WithTimeout(ctx, writeTimeout)
		defer cancel()
		return conn.Write(wctx, websocket.MessageText, msg) == nil
	}
	for {
		select {
		case msg := <-c.replies:
			if !send(msg) {
				return metrics.WSError
			}
		case msg := <-c.out:
			if !send(msg) {
				return metrics.WSError
			}
		case <-pingFailed:
			return metrics.WSPingTimeout
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

// ping pings the peer every pingInterval and closes pingFailed when a pong does not arrive in
// time. It runs on its own, so a long write or request never delays the check.
func (h *Handler) ping(ctx context.Context, conn *websocket.Conn, pingFailed chan<- struct{}) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, writeTimeout)
		err := conn.Ping(pctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				close(pingFailed)
			}
			return
		}
	}
}

// read reads client messages and hands them to the processor until the connection fails or the
// client closes it. It never blocks on a request: a client with pendingRequests requests waiting
// gets an error for the next one.
func (h *Handler) read(ctx context.Context, conn *websocket.Conn, c *client, requests chan<- []byte) error {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if typ != websocket.MessageText {
			return websocket.CloseError{Code: websocket.StatusUnsupportedData, Reason: "only JSON text messages are accepted"}
		}
		select {
		case requests <- data:
		default:
			h.tryReply(c, errorMessage{Type: typeError, Code: "rate_limited",
				Message: fmt.Sprintf("too many requests in flight; at most %d per second are handled", requestRate)})
		}
	}
}

// process handles requests in order, at most requestRate per second.
func (h *Handler) process(ctx context.Context, c *client, requests <-chan []byte) {
	t := time.NewTicker(time.Second / requestRate)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-requests:
			if err := h.handle(ctx, c, data); err != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
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
// unsubscribed again and reported. Snapshot reads share the API's database pool, so the hub
// bounds how many run at once across all connections.
func (h *Handler) snapshot(ctx context.Context, c *client, notifications, batches []uuid.UUID) error {
	if len(notifications) > 0 {
		var (
			found map[uuid.UUID]domain.Notification
			err   error
		)
		h.hub.withSnapshotSlot(ctx, func() {
			sctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
			defer cancel()
			found, err = h.store.GetNotifications(sctx, notifications)
		})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for _, id := range notifications {
			var msg any
			n, ok := found[id]
			switch {
			case err != nil:
				msg = errorMessage{Type: typeError, Code: "snapshot_failed", Message: "current status unavailable; updates follow", ID: id.String()}
			case !ok:
				h.hub.unsubscribe(c, []uuid.UUID{id}, nil)
				msg = errorMessage{Type: typeError, Code: "not_found", Message: "no notification with this id", ID: id.String()}
			default:
				msg = statusMessage{Type: typeStatus, Event: queue.NewStatusEvent(n), Snapshot: true}
			}
			if err := h.reply(ctx, c, msg); err != nil {
				return err
			}
		}
		if err != nil {
			h.logger.WarnContext(ctx, "websocket snapshot failed", "notifications", len(notifications), "err", err)
		}
	}
	for _, id := range batches {
		var (
			b   domain.Batch
			err error
		)
		h.hub.withSnapshotSlot(ctx, func() {
			sctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
			defer cancel()
			b, err = h.store.GetBatch(sctx, id)
		})
		if ctx.Err() != nil {
			return ctx.Err()
		}
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

// reply queues an answer to the client's own request. Replies have a queue of their own, so they
// never take the room events need, and unlike events they wait for room: a client that sends
// many requests is slowed down, not disconnected.
func (h *Handler) reply(ctx context.Context, c *client, msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode websocket reply: %w", err)
	}
	select {
	case c.replies <- data:
		h.metrics.Sent(messageType(msg))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryReply queues an answer if there is room, and drops it otherwise.
func (h *Handler) tryReply(c *client, msg any) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	select {
	case c.replies <- data:
		h.metrics.Sent(messageType(msg))
	default:
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

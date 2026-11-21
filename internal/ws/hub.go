// Package ws serves real-time status updates over WebSocket (B-4, ADR-0011). Every API instance
// runs one Hub: it reads every status event from notification.events, without a consumer group,
// and forwards each event to the connections subscribed to its notification id or batch id.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/queue"
)

// Limits of one connection (ADR-0011).
const (
	// MaxSubscriptions bounds the notification and batch ids one connection follows.
	MaxSubscriptions = 1000
	// MaxIDsPerMessage bounds the ids of one subscribe message; each new id costs a snapshot read.
	MaxIDsPerMessage = 100
	// sendQueue is how many messages may wait for a slow client before it is disconnected.
	sendQueue = 256
)

var errTooManySubscriptions = errors.New("too many subscriptions")

// client is the hub's side of one connection.
type client struct {
	out      chan []byte
	slow     chan struct{} // closed when out overflowed
	slowOnce sync.Once

	// Guarded by Hub.mu.
	notifications map[uuid.UUID]struct{}
	batches       map[uuid.UUID]struct{}
}

func newClient() *client {
	return &client{
		out: make(chan []byte, sendQueue), slow: make(chan struct{}),
		notifications: map[uuid.UUID]struct{}{}, batches: map[uuid.UUID]struct{}{},
	}
}

// push queues msg without blocking. A client whose queue is full is marked slow and disconnected
// by its writer: falling behind would only grow memory, and the client recovers with a snapshot
// after reconnecting.
func (c *client) push(msg []byte) bool {
	select {
	case c.out <- msg:
		return true
	default:
		c.slowOnce.Do(func() { close(c.slow) })
		return false
	}
}

// Hub routes status events to subscribed connections. It is safe for concurrent use.
type Hub struct {
	mu             sync.RWMutex
	byNotification map[uuid.UUID]map[*client]struct{}
	byBatch        map[uuid.UUID]map[*client]struct{}
	clients        map[*client]struct{}
	closing        bool

	done     chan struct{} // closed by Shutdown
	doneOnce sync.Once
	conns    sync.WaitGroup

	metrics *metrics.WebSocket
	logger  *slog.Logger
}

// NewHub returns a hub. m may be nil.
func NewHub(m *metrics.WebSocket, logger *slog.Logger) *Hub {
	return &Hub{
		byNotification: map[uuid.UUID]map[*client]struct{}{},
		byBatch:        map[uuid.UUID]map[*client]struct{}{},
		clients:        map[*client]struct{}{},
		done:           make(chan struct{}),
		metrics:        m,
		logger:         logger,
	}
}

// register adds a connection. It returns false once the hub is shutting down.
func (h *Hub) register(c *client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closing {
		return false
	}
	h.clients[c] = struct{}{}
	h.conns.Add(1)
	return true
}

// unregister removes a connection and all its subscriptions.
func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id := range c.notifications {
		remove(h.byNotification, id, c)
	}
	for id := range c.batches {
		remove(h.byBatch, id, c)
	}
	delete(h.clients, c)
	h.conns.Done()
}

// subscribe adds subscriptions and returns the ids that are new for c. If the result would
// exceed MaxSubscriptions, nothing is added.
func (h *Hub) subscribe(c *client, notifications, batches []uuid.UUID) (newNotifications, newBatches []uuid.UUID, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range notifications {
		if _, ok := c.notifications[id]; !ok && !slices.Contains(newNotifications, id) {
			newNotifications = append(newNotifications, id)
		}
	}
	for _, id := range batches {
		if _, ok := c.batches[id]; !ok && !slices.Contains(newBatches, id) {
			newBatches = append(newBatches, id)
		}
	}
	if len(c.notifications)+len(c.batches)+len(newNotifications)+len(newBatches) > MaxSubscriptions {
		return nil, nil, errTooManySubscriptions
	}
	for _, id := range newNotifications {
		c.notifications[id] = struct{}{}
		add(h.byNotification, id, c)
	}
	for _, id := range newBatches {
		c.batches[id] = struct{}{}
		add(h.byBatch, id, c)
	}
	return newNotifications, newBatches, nil
}

// unsubscribe removes subscriptions; ids c does not follow are ignored.
func (h *Hub) unsubscribe(c *client, notifications, batches []uuid.UUID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range notifications {
		if _, ok := c.notifications[id]; ok {
			delete(c.notifications, id)
			remove(h.byNotification, id, c)
		}
	}
	for _, id := range batches {
		if _, ok := c.batches[id]; ok {
			delete(c.batches, id)
			remove(h.byBatch, id, c)
		}
	}
}

// subscriptions returns the ids c follows, sorted.
func (h *Hub) subscriptions(c *client) (notifications, batches []uuid.UUID) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	notifications, batches = make([]uuid.UUID, 0, len(c.notifications)), make([]uuid.UUID, 0, len(c.batches))
	for id := range c.notifications {
		notifications = append(notifications, id)
	}
	for id := range c.batches {
		batches = append(batches, id)
	}
	cmp := func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) }
	slices.SortFunc(notifications, cmp)
	slices.SortFunc(batches, cmp)
	return notifications, batches
}

// Publish forwards e to every connection subscribed to its notification or its batch. A
// connection subscribed to both gets it once.
func (h *Hub) Publish(e queue.StatusEvent) {
	h.mu.RLock()
	var targets []*client
	for c := range h.byNotification[e.NotificationID] {
		targets = append(targets, c)
	}
	if e.BatchID != nil {
		for c := range h.byBatch[*e.BatchID] {
			if _, both := c.notifications[e.NotificationID]; !both {
				targets = append(targets, c)
			}
		}
	}
	h.mu.RUnlock()
	if len(targets) == 0 {
		return
	}
	msg, err := json.Marshal(statusMessage{Type: typeStatus, Event: e})
	if err != nil {
		h.logger.Error("encode status message", "err", err)
		return
	}
	for _, c := range targets {
		if c.push(msg) {
			h.metrics.Sent(typeStatus)
		}
	}
}

// ConsumerOptions are the kgo options of the hub's Kafka client: every partition of
// notification.events from the end, without a consumer group. Every instance sees every event,
// nothing is committed, and nothing is left on the broker when the instance goes away
// (ADR-0002 amendment).
func ConsumerOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumeTopics(queue.EventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(200 * time.Millisecond), // events are small; deliver them promptly
	}
}

// Consume reads status events from cl and publishes them until ctx ends. Fetch errors are logged
// at most every 30 s; franz-go keeps reconnecting meanwhile.
func (h *Hub) Consume(ctx context.Context, cl *kgo.Client) {
	var lastWarn time.Time
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 && time.Since(lastWarn) > 30*time.Second {
			lastWarn = time.Now()
			h.logger.Warn("reading status events failed; live updates are delayed", "err", errs[0].Err, "topic", errs[0].Topic)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			h.metrics.Received()
			e, err := queue.DecodeStatusEvent(r)
			if err != nil {
				h.logger.Warn("skipping undecodable status event", "err", err)
				return
			}
			h.Publish(e)
		})
	}
}

// Shutdown closes every connection with 1001 (going away) and refuses new ones. Clients
// reconnect through the load balancer to another instance.
func (h *Hub) Shutdown() {
	h.doneOnce.Do(func() {
		h.mu.Lock()
		h.closing = true
		h.mu.Unlock()
		close(h.done)
	})
}

// Wait blocks until every connection has ended or ctx ends.
func (h *Hub) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() { h.conns.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func add(m map[uuid.UUID]map[*client]struct{}, id uuid.UUID, c *client) {
	if m[id] == nil {
		m[id] = map[*client]struct{}{}
	}
	m[id][c] = struct{}{}
}

func remove(m map[uuid.UUID]map[*client]struct{}, id uuid.UUID, c *client) {
	delete(m[id], c)
	if len(m[id]) == 0 {
		delete(m, id)
	}
}

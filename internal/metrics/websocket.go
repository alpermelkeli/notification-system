package metrics

import "github.com/prometheus/client_golang/prometheus"

// Reasons a WebSocket connection ends.
const (
	WSClosedByClient = "client"
	WSSlowConsumer   = "slow_consumer"
	WSPingTimeout    = "ping_timeout"
	WSShutdown       = "shutdown"
	WSError          = "error"
)

// WebSocket holds the metrics of the API's WebSocket status stream (B-4, ADR-0011).
type WebSocket struct {
	connections prometheus.Gauge
	messages    *prometheus.CounterVec
	disconnects *prometheus.CounterVec
	received    prometheus.Counter
}

// NewWebSocket registers the WebSocket metrics on reg.
func NewWebSocket(reg prometheus.Registerer) *WebSocket {
	m := &WebSocket{
		connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "ws_connections",
			Help: "Open WebSocket connections on this API instance.",
		}),
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "ws_messages_total",
			Help: "Messages sent to WebSocket clients, by type (status, batch, subscribed, error).",
		}, []string{"type"}),
		disconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "ws_disconnects_total",
			Help: "Closed WebSocket connections, by reason (client, slow_consumer, ping_timeout, shutdown, error).",
		}, []string{"reason"}),
		received: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "ws_events_received_total",
			Help: "Status events read from notification.events by this instance's hub.",
		}),
	}
	reg.MustRegister(m.connections, m.messages, m.disconnects, m.received)
	return m
}

// Connected counts an opened connection.
func (m *WebSocket) Connected() {
	if m != nil {
		m.connections.Inc()
	}
}

// Disconnected counts a closed connection and why it closed.
func (m *WebSocket) Disconnected(reason string) {
	if m != nil {
		m.connections.Dec()
		m.disconnects.WithLabelValues(reason).Inc()
	}
}

// Sent counts a message of the given type sent to a client.
func (m *WebSocket) Sent(msgType string) {
	if m != nil {
		m.messages.WithLabelValues(msgType).Inc()
	}
}

// Received counts a status event read from Kafka.
func (m *WebSocket) Received() {
	if m != nil {
		m.received.Inc()
	}
}

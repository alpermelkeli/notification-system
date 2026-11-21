package telemetry

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
)

// recordCarrier adapts Kafka record headers to the OpenTelemetry propagation API.
type recordCarrier struct{ r *kgo.Record }

func (c recordCarrier) Get(key string) string {
	for _, h := range c.r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c recordCarrier) Set(key, value string) {
	for i, h := range c.r.Headers {
		if h.Key == key {
			c.r.Headers[i].Value = []byte(value)
			return
		}
	}
	c.r.Headers = append(c.r.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c recordCarrier) Keys() []string {
	keys := make([]string, len(c.r.Headers))
	for i, h := range c.r.Headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectRecord writes the trace context of ctx into r's headers (traceparent).
func InjectRecord(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, recordCarrier{r})
}

// ExtractRecord returns ctx with the trace context of r's headers as the remote parent.
func ExtractRecord(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, recordCarrier{r})
}

package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/telemetry"
)

func kafkaClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	if kafkaAddr == "" {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(kafkaAddr)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func createTopic(t *testing.T, adm *kadm.Client, topic string, partitions int32) {
	t.Helper()
	resp, err := adm.CreateTopic(context.Background(), partitions, 1, nil, topic)
	if err != nil || resp.Err != nil {
		t.Fatalf("create topic %s: %v %v", topic, err, resp.Err)
	}
}

// TestPublishAndCommittedOffsets publishes work records, consumes and commits some of them as a
// worker group would, and checks that the reaper's offset source sees exactly those commits.
func TestPublishAndCommittedOffsets(t *testing.T) {
	cl := kafkaClient(t, ProducerOptions()...)
	adm := kadm.NewClient(cl)
	createTopic(t, adm, queue.Topic(domain.ChannelSMS, domain.PriorityHigh), 3)
	createTopic(t, adm, queue.Topic(domain.ChannelSMS, domain.PriorityLow), 1)

	now := time.Now()
	var ns []domain.Notification
	for i := range 20 {
		id := domain.NewIDAt(now)
		p := domain.PriorityHigh
		if i%4 == 0 {
			p = domain.PriorityLow
		}
		ns = append(ns, domain.Notification{
			ID: id, CreatedAt: domain.IDTime(id), Channel: domain.ChannelSMS, Priority: p,
			NextAttemptAt: now, CorrelationID: "corr",
		})
	}
	const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ns[0].TraceParent = parent
	spans := recordSpans(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reg := metrics.NewRegistry()
	positions, err := NewPublisher(cl, metrics.NewDispatch(reg)).Publish(ctx, ns)
	if err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, reg, "notifier_dispatched_total", "priority", "high"); got != 15 {
		t.Errorf("dispatched high = %v, want 15", got)
	}
	ended := spans.Ended()
	if len(ended) != len(ns) {
		t.Fatalf("%d publish spans, want %d", len(ended), len(ns))
	}
	var first sdktrace.ReadOnlySpan
	for _, s := range ended {
		if s.Parent().TraceID().String() == "4bf92f3577b34da6a3ce929d0e0e4736" {
			first = s
		}
	}
	if first == nil || first.Parent().SpanID().String() != "00f067aa0ba902b7" || first.SpanKind() != trace.SpanKindProducer {
		t.Fatalf("no producer span continues the stored traceparent: %v", first)
	}

	// Read everything back and check that each position points at its own notification.
	consumer := kafkaClient(t,
		kgo.ConsumerGroup(queue.Group(domain.ChannelSMS)),
		kgo.ConsumeTopics(queue.Topics(domain.ChannelSMS)[0], queue.Topics(domain.ChannelSMS)[2]),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	type pos struct {
		topic     string
		partition int32
		offset    int64
	}
	byPosition := map[pos]*kgo.Record{}
	for len(byPosition) < len(ns) {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			t.Fatal(err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			byPosition[pos{r.Topic, r.Partition, r.Offset}] = r
		})
	}
	for i, n := range ns {
		p := positions[i]
		r := byPosition[pos{queue.Topic(n.Channel, n.Priority), p.Partition, p.Offset}]
		if r == nil {
			t.Fatalf("no record at the position returned for notification %d: %+v", i, p)
		}
		m, err := queue.Decode(r)
		if err != nil || m.ID != n.ID || queue.CorrelationID(r) != "corr" {
			t.Fatalf("position %+v holds %+v (%v), want notification %s", p, m, err, n.ID)
		}
		if i == 0 {
			sc := trace.SpanContextFromContext(telemetry.ExtractRecord(ctx, r))
			if sc.TraceID() != first.SpanContext().TraceID() || sc.SpanID() != first.SpanContext().SpanID() {
				t.Errorf("record carries %v, want the publish span %v", sc, first.SpanContext())
			}
		}
	}

	// Commit up to (excluding) the record of ns[1] in its partition, as a worker watermark would.
	target := queue.Topic(ns[1].Channel, ns[1].Priority)
	commit := map[string]map[int32]kgo.EpochOffset{
		target: {positions[1].Partition: {Epoch: -1, Offset: positions[1].Offset}},
	}
	if err := commitSync(ctx, consumer, commit); err != nil {
		t.Fatal(err)
	}

	got, err := NewKafkaOffsets(adm).CommittedOffsets(ctx, queue.Group(domain.ChannelSMS))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[target]) != 1 || got[target][positions[1].Partition] != positions[1].Offset {
		t.Errorf("committed offsets = %v, want only %s/%d=%d", got, target, positions[1].Partition, positions[1].Offset)
	}

	// Lag: the committed partition counts from its commit, the others from their start.
	perTopic := map[string]int64{}
	for _, n := range ns {
		perTopic[queue.Topic(n.Channel, n.Priority)]++
	}
	lag, err := NewKafkaOffsets(adm).Lag(ctx, queue.Group(domain.ChannelSMS))
	if err != nil {
		t.Fatal(err)
	}
	low := queue.Topic(domain.ChannelSMS, domain.PriorityLow)
	if g := lag[queue.Group(domain.ChannelSMS)]; g[target] != perTopic[target]-positions[1].Offset || g[low] != perTopic[low] {
		t.Errorf("lag = %v, want %s=%d and %s=%d", g, target, perTopic[target]-positions[1].Offset, low, perTopic[low])
	}

	none, err := NewKafkaOffsets(adm).CommittedOffsets(ctx, "workers.nobody")
	if err != nil || len(none) != 0 {
		t.Errorf("unknown group: %v, %v", none, err)
	}
}

func commitSync(ctx context.Context, cl *kgo.Client, offsets map[string]map[int32]kgo.EpochOffset) error {
	var commitErr error
	cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, topic := range resp.Topics {
			for _, p := range topic.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					commitErr = err
				}
			}
		}
	})
	return commitErr
}

// TestPublishFailsWithinTheTimeout: a record that cannot be delivered (here its topic does not
// exist and auto-creation is off) must fail the batch within the publish timeout, so the
// dispatcher's transaction releases its row locks instead of waiting forever.
func TestPublishFailsWithinTheTimeout(t *testing.T) {
	cl := kafkaClient(t, ProducerOptions()...)
	id := domain.NewIDAt(time.Now())
	p := NewPublisher(cl, nil)
	p.timeout = time.Second
	start := time.Now()
	_, err := p.Publish(context.Background(), []domain.Notification{{
		ID: id, CreatedAt: domain.IDTime(id), Channel: domain.ChannelPush, Priority: domain.PriorityHigh,
		NextAttemptAt: time.Now(),
	}})
	if err == nil {
		t.Fatal("publishing to a missing topic succeeded")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("publish failed after %v, want about the 1s timeout", took)
	}
}

// recordSpans installs a recording tracer provider for the test.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
	return rec
}

// counterValue returns the value of the counter series of name that has the label.
func counterValue(t *testing.T, reg *prometheus.Registry, name, label, value string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

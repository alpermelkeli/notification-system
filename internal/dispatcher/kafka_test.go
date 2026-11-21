package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/queue"
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	positions, err := NewPublisher(cl).Publish(ctx, ns)
	if err != nil {
		t.Fatal(err)
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

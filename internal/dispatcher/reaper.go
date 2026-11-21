package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/metrics"
	"github.com/alpermelkeli/notification-system/internal/queue"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// ReaperStore is the persistence the reaper needs. *storage.Store implements it.
type ReaperStore interface {
	ReapExpiredLeases(ctx context.Context, maxAttempts, limit int) (storage.ReapResult, error)
	RequeueConsumed(ctx context.Context, committed []storage.CommittedOffset, grace time.Duration, limit int) ([]domain.Notification, error)
	RequeueStuck(ctx context.Context, olderThan time.Duration, limit int) ([]domain.Notification, error)
}

// DeadLetterPublisher publishes the dead letters of notifications that moved to failed.
// *queue.DeadLetters implements it.
type DeadLetterPublisher interface {
	Publish(ctx context.Context, ns ...domain.Notification) error
}

// OffsetSource returns a consumer group's committed offsets, by topic and partition. Partitions
// without a committed offset are left out.
type OffsetSource interface {
	CommittedOffsets(ctx context.Context, group string) (map[string]map[int32]int64, error)
}

// KafkaOffsets reads committed offsets through the Kafka admin API.
type KafkaOffsets struct {
	adm *kadm.Client
}

// NewKafkaOffsets returns an OffsetSource on adm.
func NewKafkaOffsets(adm *kadm.Client) *KafkaOffsets { return &KafkaOffsets{adm: adm} }

// CommittedOffsets implements OffsetSource.
func (k *KafkaOffsets) CommittedOffsets(ctx context.Context, group string) (map[string]map[int32]int64, error) {
	resp, err := k.adm.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets of %s: %w", group, err)
	}
	out := map[string]map[int32]int64{}
	var errs []error
	resp.Each(func(o kadm.OffsetResponse) {
		switch {
		case o.Err != nil:
			errs = append(errs, fmt.Errorf("%s/%d: %w", o.Topic, o.Partition, o.Err))
		case o.At >= 0:
			if out[o.Topic] == nil {
				out[o.Topic] = map[int32]int64{}
			}
			out[o.Topic][o.Partition] = o.At
		}
	})
	// Partitions that answered are still usable; the others are checked on the next run.
	return out, errors.Join(errs...)
}

// ReaperOptions configures a Reaper.
type ReaperOptions struct {
	// Interval between runs. Default 30s.
	Interval time.Duration
	// Grace is how long a queued row is left alone even if its offset is behind the committed
	// one, so a dispatch commit racing a fast worker is not mistaken for loss. Default 1m.
	Grace time.Duration
	// StuckAfter is the age fallback for queued rows the offset rule cannot judge. Default 24h.
	StuckAfter time.Duration
	// MaxAttempts is the attempt budget: an expired lease on the last attempt fails the
	// notification instead of retrying it. Default 5.
	MaxAttempts int
	// Limit bounds the rows changed per statement. Default 1000.
	Limit int
	// Channels whose work topics are checked. Default all.
	Channels []domain.Channel
	// Metrics counts recoveries and dead letters. Optional.
	Metrics *metrics.Dispatch
}

// Reaper recovers notifications that would otherwise never be delivered (ADR-0003):
//   - processing rows whose lease expired (the worker crashed or stalled) go to retrying, or to
//     failed with a dead letter when that was their last attempt;
//   - queued rows whose Kafka record the channel's consumer group has already committed past
//     go back to pending (offset rule of the ADR-0003 amendment);
//   - queued rows older than StuckAfter go back to pending (fallback).
//
// Every replica runs it. Correctness does not depend on running it once: each statement locks
// its rows with FOR UPDATE SKIP LOCKED, so concurrent reapers never change a row twice, and the
// offset reads are idempotent. An advisory lock would only save a few cheap requests.
type Reaper struct {
	store   ReaperStore
	offsets OffsetSource
	dead    DeadLetterPublisher
	logger  *slog.Logger
	opts    ReaperOptions
}

// NewReaper returns a reaper.
func NewReaper(store ReaperStore, offsets OffsetSource, dead DeadLetterPublisher, logger *slog.Logger, opts ReaperOptions) *Reaper {
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Second
	}
	if opts.Grace <= 0 {
		opts.Grace = time.Minute
	}
	if opts.StuckAfter <= 0 {
		opts.StuckAfter = 24 * time.Hour
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.Limit <= 0 {
		opts.Limit = 1000
	}
	if len(opts.Channels) == 0 {
		opts.Channels = domain.Channels
	}
	return &Reaper{store: store, offsets: offsets, dead: dead, logger: logger, opts: opts}
}

// ReapReport counts what one run changed.
type ReapReport struct {
	// ExpiredLeases counts expired leases retried; ExhaustedLeases those failed on their last
	// attempt.
	ExpiredLeases   int
	ExhaustedLeases int
	Consumed        int
	Stuck           int
}

// maxRounds bounds the statements per step and run, so one run cannot hold a replica for long
// after a mass failure; the next run continues.
const maxRounds = 10

// RunOnce performs one reaper run. Steps are independent: a failing step does not stop the
// others, and all errors are returned joined.
func (r *Reaper) RunOnce(ctx context.Context) (ReapReport, error) {
	var (
		rep  ReapReport
		errs []error
	)
	_, err := r.drain(ctx, func() (int, error) {
		res, err := r.store.ReapExpiredLeases(ctx, r.opts.MaxAttempts, r.opts.Limit)
		rep.ExpiredLeases += len(res.Retried)
		rep.ExhaustedLeases += len(res.Failed)
		r.deadLetter(ctx, res.Failed)
		return len(res.Retried) + len(res.Failed), err
	})
	errs = append(errs, err)

	committed, err := r.committed(ctx)
	errs = append(errs, err)
	if len(committed) > 0 {
		n, err := r.drain(ctx, func() (int, error) {
			ns, err := r.store.RequeueConsumed(ctx, committed, r.opts.Grace, r.opts.Limit)
			return len(ns), err
		})
		rep.Consumed = n
		errs = append(errs, err)
	}

	n, err := r.drain(ctx, func() (int, error) {
		ns, err := r.store.RequeueStuck(ctx, r.opts.StuckAfter, r.opts.Limit)
		return len(ns), err
	})
	rep.Stuck = n
	errs = append(errs, err)

	m := r.opts.Metrics
	m.Reaped(metrics.ReapExpiredLease, rep.ExpiredLeases)
	m.Reaped(metrics.ReapExhaustedLease, rep.ExhaustedLeases)
	m.Reaped(metrics.ReapConsumed, rep.Consumed)
	m.Reaped(metrics.ReapStuck, rep.Stuck)
	return rep, errors.Join(errs...)
}

// deadLetter publishes the dead letters of failed rows, best effort: the rows are committed as
// failed and listed by the API whatever happens here (ADR-0005).
func (r *Reaper) deadLetter(ctx context.Context, failed []domain.Notification) {
	if len(failed) == 0 || r.dead == nil {
		return
	}
	err := r.dead.Publish(ctx, failed...)
	r.opts.Metrics.DeadLetters(len(failed), err)
	if err != nil {
		r.logger.Warn("dead letters not published", "count", len(failed), "err", err)
	}
}

func (r *Reaper) drain(ctx context.Context, step func() (int, error)) (int, error) {
	total := 0
	for range maxRounds {
		n, err := step()
		total += n
		if err != nil || n < r.opts.Limit || ctx.Err() != nil {
			return total, err
		}
	}
	return total, nil
}

// committed collects the committed offsets of every work partition of the configured channels.
func (r *Reaper) committed(ctx context.Context) ([]storage.CommittedOffset, error) {
	var (
		out  []storage.CommittedOffset
		errs []error
	)
	for _, c := range r.opts.Channels {
		offsets, err := r.offsets.CommittedOffsets(ctx, queue.Group(c))
		if err != nil {
			errs = append(errs, err)
		}
		for topic, parts := range offsets {
			tc, tp, ok := queue.ParseTopic(topic)
			if !ok || tc != c {
				continue
			}
			for partition, offset := range parts {
				out = append(out, storage.CommittedOffset{Channel: c, Priority: tp, Partition: partition, Offset: offset})
			}
		}
	}
	return out, errors.Join(errs...)
}

// Run reaps every Interval until ctx ends.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.opts.Interval)
	defer ticker.Stop()
	for {
		rep, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// Every step is retried on the next run; right after start-up the group coordinator
			// is often not available yet.
			r.logger.Warn("reaper run incomplete; retrying next run", "err", err)
		}
		if rep != (ReapReport{}) {
			r.logger.Warn("reaper recovered notifications",
				"expired_leases", rep.ExpiredLeases, "exhausted_leases", rep.ExhaustedLeases, "consumed_without_claim", rep.Consumed, "stuck_in_queued", rep.Stuck)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

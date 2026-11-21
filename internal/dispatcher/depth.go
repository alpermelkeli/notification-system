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

// DepthStore reads the PostgreSQL backlog. *storage.Store implements it.
type DepthStore interface {
	QueueDepth(ctx context.Context) (storage.QueueDepth, error)
}

// LagSource returns the uncommitted records of consumer groups, by group and topic.
type LagSource interface {
	Lag(ctx context.Context, groups ...string) (map[string]map[string]int64, error)
}

// Lag implements LagSource: per group and topic, the sum over partitions of end offset minus
// committed offset. Partitions whose lag cannot be computed are left out and reported in the
// error.
func (k *KafkaOffsets) Lag(ctx context.Context, groups ...string) (map[string]map[string]int64, error) {
	lags, err := k.adm.Lag(ctx, groups...)
	if err != nil {
		return nil, fmt.Errorf("fetch consumer lag: %w", err)
	}
	out := map[string]map[string]int64{}
	var errs []error
	lags.Each(func(l kadm.DescribedGroupLag) {
		if err := l.Error(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", l.Group, err))
			return
		}
		byTopic := map[string]int64{}
		for topic, parts := range l.Lag {
			for _, p := range parts {
				if p.Err != nil || p.Lag < 0 {
					errs = append(errs, fmt.Errorf("%s %s/%d: lag unknown: %w", l.Group, topic, p.Partition, p.Err))
					continue
				}
				byTopic[topic] += p.Lag
			}
		}
		out[l.Group] = byTopic
	})
	return out, errors.Join(errs...)
}

// DepthMonitor samples the queue depth for the metrics endpoint (FR-4.1): the unfinished rows
// in PostgreSQL by status, the due backlog and its age, and the Kafka consumer lag of every
// channel's workers. Sampling on an interval instead of on scrape keeps the cost fixed however
// often Prometheus scrapes. Every dispatcher replica reports the same values, so dashboards take
// max() over instances.
type DepthMonitor struct {
	store    DepthStore
	lag      LagSource
	metrics  *metrics.Dispatch
	logger   *slog.Logger
	interval time.Duration
	channels []domain.Channel
	now      func() time.Time
}

// NewDepthMonitor returns a monitor sampling every interval (default 15s).
func NewDepthMonitor(store DepthStore, lag LagSource, m *metrics.Dispatch, logger *slog.Logger, interval time.Duration) *DepthMonitor {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &DepthMonitor{
		store: store, lag: lag, metrics: m, logger: logger, interval: interval,
		channels: domain.Channels, now: time.Now,
	}
}

// depthTimeout bounds one sample.
const depthTimeout = 5 * time.Second

// Run samples until ctx ends. Failed samples keep the gauges at their last value and are
// counted; they are logged at debug level, since readiness already reports an unavailable
// dependency.
func (d *DepthMonitor) Run(ctx context.Context) {
	t := time.NewTicker(d.interval)
	defer t.Stop()
	for {
		if err := d.SampleOnce(ctx); err != nil && ctx.Err() == nil {
			d.logger.Debug("queue depth sample incomplete", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// SampleOnce takes one sample of both sources.
func (d *DepthMonitor) SampleOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, depthTimeout)
	defer cancel()

	var errs []error
	depth, err := d.store.QueueDepth(ctx)
	if err != nil {
		d.metrics.DepthFailed("postgres")
		errs = append(errs, err)
	} else {
		age := 0.0
		if depth.OldestDue != nil {
			age = d.now().Sub(*depth.OldestDue).Seconds()
		}
		d.metrics.Depth(depth.ByStatus, depth.Due, age)
	}

	groups := make([]string, len(d.channels))
	for i, c := range d.channels {
		groups[i] = queue.Group(c)
	}
	lags, err := d.lag.Lag(ctx, groups...)
	if err != nil {
		d.metrics.DepthFailed("kafka")
		errs = append(errs, err)
	}
	for _, c := range d.channels {
		g := queue.Group(c)
		for _, topic := range queue.Topics(c) {
			// A topic is missing when its group has no member and no commit on it yet.
			if lag, ok := lags[g][topic]; ok {
				d.metrics.Lag(g, topic, lag)
			}
		}
	}
	return errors.Join(errs...)
}

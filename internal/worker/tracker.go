package worker

import (
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// tracker computes per-partition commit watermarks (ADR-0002). Records of a partition finish out
// of order because sends run concurrently; the committable offset is the one after the highest
// contiguous finished record, so a crash never skips an unfinished record.
type tracker struct {
	mu    sync.Mutex
	parts map[topicPartition]*partition
}

type topicPartition struct {
	topic     string
	partition int32
}

// partition is the state of one assigned partition. A revoked partition's state is detached
// from the tracker, so late completions from the previous assignment are ignored.
type partition struct {
	tp      topicPartition
	pending []pendingRecord // ascending offsets, not yet below the watermark
	// next is the offset to commit (one past the last contiguous finished record), with the
	// leader epoch of that record; -1 until a record finishes.
	next      int64
	epoch     int32
	committed int64
	inFlight  int
	detached  bool
}

type pendingRecord struct {
	offset int64
	epoch  int32
	done   bool
}

func newTracker() *tracker { return &tracker{parts: map[topicPartition]*partition{}} }

// add registers a fetched record. Records of one partition are fetched in offset order.
func (t *tracker) add(r *kgo.Record) *partition {
	t.mu.Lock()
	defer t.mu.Unlock()
	tp := topicPartition{r.Topic, r.Partition}
	p, ok := t.parts[tp]
	if !ok {
		p = &partition{tp: tp, next: -1, committed: -1}
		t.parts[tp] = p
	}
	p.pending = append(p.pending, pendingRecord{offset: r.Offset, epoch: r.LeaderEpoch})
	return p
}

// start marks a record of p as being processed.
func (t *tracker) start(p *partition) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p.inFlight++
}

// done marks the record at offset finished (its outcome or skip is persisted). started tells
// whether start was called for it.
func (t *tracker) done(p *partition, offset int64, started bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if started {
		p.inFlight--
	}
	if p.detached {
		return
	}
	i, found := slices.BinarySearchFunc(p.pending, offset, func(r pendingRecord, o int64) int {
		switch {
		case r.offset < o:
			return -1
		case r.offset > o:
			return 1
		}
		return 0
	})
	if !found {
		return
	}
	p.pending[i].done = true
	n := 0
	for n < len(p.pending) && p.pending[n].done {
		p.next, p.epoch = p.pending[n].offset+1, p.pending[n].epoch
		n++
	}
	p.pending = slices.Delete(p.pending, 0, n)
}

// committable returns the watermarks that moved since the last successful commit.
func (t *tracker) committable(only map[string][]int32) map[string]map[int32]kgo.EpochOffset {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]map[int32]kgo.EpochOffset{}
	for tp, p := range t.parts {
		if only != nil && !slices.Contains(only[tp.topic], tp.partition) {
			continue
		}
		if p.next > p.committed {
			if out[tp.topic] == nil {
				out[tp.topic] = map[int32]kgo.EpochOffset{}
			}
			out[tp.topic][tp.partition] = kgo.EpochOffset{Epoch: p.epoch, Offset: p.next}
		}
	}
	return out
}

// committed records a successful commit of offsets.
func (t *tracker) committed(offsets map[string]map[int32]kgo.EpochOffset) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, parts := range offsets {
		for partition, eo := range parts {
			if p, ok := t.parts[topicPartition{topic, partition}]; ok && eo.Offset > p.committed {
				p.committed = eo.Offset
			}
		}
	}
}

// inFlight returns the number of started, unfinished records in the given partitions.
func (t *tracker) inFlight(parts map[string][]int32) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for topic, ps := range parts {
		for _, partition := range ps {
			if p, ok := t.parts[topicPartition{topic, partition}]; ok {
				n += p.inFlight
			}
		}
	}
	return n
}

// detach forgets the given partitions; completions still in flight for them are ignored.
func (t *tracker) detach(parts map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for topic, ps := range parts {
		for _, partition := range ps {
			tp := topicPartition{topic, partition}
			if p, ok := t.parts[tp]; ok {
				p.detached = true
				delete(t.parts, tp)
			}
		}
	}
}

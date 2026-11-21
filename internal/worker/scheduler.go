package worker

import (
	"container/heap"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/queue"
)

// Policy is the capped-aging priority policy of ADR-0006:
//
//	effective = base + min(agingRate × seconds waited since due_at, cap)
//
// The head with the highest effective priority is sent next. With the defaults nothing ever
// overtakes high (its cap is 0 and the best non-high score is 95), and low overtakes a fresh
// normal after about 20 s. Every MinShareEvery-th pick goes to the best non-high head, if one is
// waiting, so a sustained high flood cannot starve the other lanes.
type Policy struct {
	Base map[domain.Priority]float64
	Cap  map[domain.Priority]float64
	// AgingRate is in points per second waited.
	AgingRate float64
	// MinShareEvery reserves one pick in this many for non-high traffic. 0 disables it.
	MinShareEvery int
}

// DefaultPolicy returns the weights from ADR-0006.
func DefaultPolicy() Policy {
	return Policy{
		Base:          map[domain.Priority]float64{domain.PriorityHigh: 100, domain.PriorityNormal: 50, domain.PriorityLow: 10},
		Cap:           map[domain.Priority]float64{domain.PriorityHigh: 0, domain.PriorityNormal: 45, domain.PriorityLow: 80},
		AgingRate:     2,
		MinShareEvery: 10,
	}
}

// effective returns the effective priority of a record of priority p that became due at dueAt.
func (p Policy) effective(pr domain.Priority, dueAt, now time.Time) float64 {
	wait := max(now.Sub(dueAt).Seconds(), 0)
	return p.Base[pr] + min(p.AgingRate*wait, p.Cap[pr])
}

// record is a decoded work record and its partition state.
type record struct {
	kr            *kgo.Record
	msg           queue.Message
	correlationID string
	part          *partition
	// tokenAt is when the record's rate-limit token was granted.
	tokenAt time.Time
}

// item is one fetched work record waiting in a priority buffer.
type item struct {
	rec   record
	dueAt time.Time
	seq   uint64 // arrival order, the last tie-breaker
}

// itemHeap is a min-heap on due_at: records from several partitions arrive interleaved, so
// arrival order is not age order (ADR-0006).
type itemHeap []*item

func (h itemHeap) Len() int { return len(h) }
func (h itemHeap) Less(i, j int) bool {
	if !h[i].dueAt.Equal(h[j].dueAt) {
		return h[i].dueAt.Before(h[j].dueAt)
	}
	return h[i].seq < h[j].seq
}
func (h itemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *itemHeap) Push(x any)   { *h = append(*h, x.(*item)) }
func (h *itemHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return it
}

// scheduler holds the three priority buffers of one channel and decides what is sent next. It
// is not safe for concurrent use; the engine serialises access.
type scheduler struct {
	policy Policy
	queues map[domain.Priority]*itemHeap
	picks  uint64
	seq    uint64
}

func newScheduler(p Policy) *scheduler {
	s := &scheduler{policy: p, queues: map[domain.Priority]*itemHeap{}}
	for _, pr := range domain.Priorities {
		s.queues[pr] = &itemHeap{}
	}
	return s
}

// push adds a record and returns the length of its buffer.
func (s *scheduler) push(pr domain.Priority, it *item) int {
	s.seq++
	it.seq = s.seq
	heap.Push(s.queues[pr], it)
	return s.queues[pr].Len()
}

func (s *scheduler) len(pr domain.Priority) int { return s.queues[pr].Len() }

func (s *scheduler) empty() bool {
	for _, q := range s.queues {
		if q.Len() > 0 {
			return false
		}
	}
	return true
}

// pick removes and returns the record to send next and its priority, or nil if every buffer is
// empty. Call it only after a rate-limit token was granted, so a record that arrives while
// waiting for the token still competes (ADR-0006).
func (s *scheduler) pick(now time.Time) (*item, domain.Priority) {
	if s.empty() {
		return nil, ""
	}
	s.picks++
	candidates := domain.Priorities
	if s.policy.MinShareEvery > 0 && s.picks%uint64(s.policy.MinShareEvery) == 0 &&
		(s.len(domain.PriorityNormal) > 0 || s.len(domain.PriorityLow) > 0) {
		candidates = []domain.Priority{domain.PriorityNormal, domain.PriorityLow}
	}

	var (
		best      domain.Priority
		bestScore float64
		bestHead  *item
	)
	for _, pr := range candidates {
		q := s.queues[pr]
		if q.Len() == 0 {
			continue
		}
		head := (*q)[0]
		score := s.policy.effective(pr, head.dueAt, now)
		if bestHead == nil || score > bestScore ||
			(score == bestScore && (head.dueAt.Before(bestHead.dueAt) ||
				(head.dueAt.Equal(bestHead.dueAt) && head.seq < bestHead.seq))) {
			best, bestScore, bestHead = pr, score, head
		}
	}
	return heap.Pop(s.queues[best]).(*item), best
}

// remove drops every buffered record for which drop returns true and returns them. It is used
// when partitions are revoked: their unstarted records belong to the new owner.
func (s *scheduler) remove(drop func(*item) bool) []*item {
	var removed []*item
	for _, q := range s.queues {
		kept := (*q)[:0]
		for _, it := range *q {
			if drop(it) {
				removed = append(removed, it)
			} else {
				kept = append(kept, it)
			}
		}
		for i := len(kept); i < len(*q); i++ {
			(*q)[i] = nil
		}
		*q = kept
		heap.Init(q)
	}
	return removed
}

package worker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func rec(partition int32, offset int64) *kgo.Record {
	return &kgo.Record{Topic: "t", Partition: partition, Offset: offset, LeaderEpoch: 3}
}

func TestWatermarkAdvancesOnlyOverContiguousRecords(t *testing.T) {
	tr := newTracker()
	var p *partition
	for o := int64(5); o <= 8; o++ {
		p = tr.add(rec(0, o))
	}
	if got := tr.committable(nil); len(got) != 0 {
		t.Fatalf("nothing finished yet, committable = %v", got)
	}
	tr.done(p, 6, false)
	tr.done(p, 7, false)
	if got := tr.committable(nil); len(got) != 0 {
		t.Fatalf("offset 5 is unfinished, committable = %v", got)
	}
	tr.done(p, 5, false)
	got := tr.committable(nil)
	if eo := got["t"][0]; eo.Offset != 8 || eo.Epoch != 3 {
		t.Fatalf("committable = %v, want offset 8 (next after 7)", got)
	}
	tr.committed(got)
	if again := tr.committable(nil); len(again) != 0 {
		t.Errorf("an unchanged watermark is committed again: %v", again)
	}
	tr.done(p, 8, false)
	if got := tr.committable(nil); got["t"][0].Offset != 9 {
		t.Errorf("committable = %v, want 9", got)
	}
}

func TestTrackerFiltersInFlightAndDetach(t *testing.T) {
	tr := newTracker()
	a := tr.add(rec(0, 1))
	b := tr.add(rec(1, 1))
	tr.start(a)
	tr.start(b)
	tr.done(b, 1, true)

	if n := tr.inFlight(map[string][]int32{"t": {0, 1}}); n != 1 {
		t.Errorf("in flight = %d, want 1", n)
	}
	if got := tr.committable(map[string][]int32{"t": {0}}); len(got) != 0 {
		t.Errorf("partition 0 has nothing finished: %v", got)
	}
	if got := tr.committable(map[string][]int32{"t": {1}}); got["t"][1].Offset != 2 || len(got["t"]) != 1 {
		t.Errorf("only partition 1 requested: %v", got)
	}

	tr.detach(map[string][]int32{"t": {0}})
	tr.done(a, 1, true) // late completion from the previous assignment
	if a.inFlight != 0 {
		t.Errorf("in-flight count of the detached partition = %d", a.inFlight)
	}
	// A re-assigned partition starts fresh and ignores the old state.
	a2 := tr.add(rec(0, 1))
	if a2 == a || tr.committable(map[string][]int32{"t": {0}})["t"] != nil {
		t.Error("re-assigned partition must not inherit the detached state")
	}
}

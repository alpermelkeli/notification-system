package worker

import (
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

var t0 = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func push(s *scheduler, pr domain.Priority, dueAt time.Time, label string) {
	s.push(pr, &item{dueAt: dueAt, rec: record{correlationID: label}})
}

func pickLabel(s *scheduler, now time.Time) string {
	it, _ := s.pick(now)
	if it == nil {
		return ""
	}
	return it.rec.correlationID
}

func TestEffectivePriority(t *testing.T) {
	p := DefaultPolicy()
	cases := []struct {
		pr   domain.Priority
		wait time.Duration
		want float64
	}{
		{domain.PriorityHigh, 0, 100},
		{domain.PriorityHigh, time.Hour, 100}, // high never ages
		{domain.PriorityNormal, 0, 50},
		{domain.PriorityNormal, 10 * time.Second, 70},
		{domain.PriorityNormal, time.Hour, 95}, // capped
		{domain.PriorityLow, 20 * time.Second, 50},
		{domain.PriorityLow, time.Hour, 90},
		{domain.PriorityLow, -time.Minute, 10}, // a future due_at does not count as waiting
	}
	for _, c := range cases {
		if got := p.effective(c.pr, t0.Add(-c.wait), t0); got != c.want {
			t.Errorf("effective(%s, waited %v) = %v, want %v", c.pr, c.wait, got, c.want)
		}
	}
}

func TestPickHighFirstAndAging(t *testing.T) {
	p := DefaultPolicy()
	p.MinShareEvery = 0
	s := newScheduler(p)
	push(s, domain.PriorityLow, t0.Add(-time.Hour), "old-low")
	push(s, domain.PriorityNormal, t0.Add(-time.Hour), "old-normal")
	push(s, domain.PriorityHigh, t0, "fresh-high")
	push(s, domain.PriorityNormal, t0, "fresh-normal")
	push(s, domain.PriorityLow, t0.Add(-21*time.Second), "aged-low") // 52 > fresh normal's 50

	want := []string{"fresh-high", "old-normal", "old-low", "aged-low", "fresh-normal"}
	for i, w := range want {
		if got := pickLabel(s, t0); got != w {
			t.Fatalf("pick %d = %q, want %q", i, got, w)
		}
	}
	if pickLabel(s, t0) != "" || !s.empty() {
		t.Error("scheduler should be empty")
	}
}

func TestPickOldestWithinPriority(t *testing.T) {
	s := newScheduler(DefaultPolicy())
	// Arrival order differs from due order, as with records from several partitions.
	push(s, domain.PriorityHigh, t0.Add(-1*time.Second), "b")
	push(s, domain.PriorityHigh, t0.Add(-3*time.Second), "a")
	push(s, domain.PriorityHigh, t0.Add(-1*time.Second), "c") // same due_at as b: arrival order
	for _, w := range []string{"a", "b", "c"} {
		if got := pickLabel(s, t0); got != w {
			t.Fatalf("got %q, want %q", got, w)
		}
	}
}

// TestMinimumShareUnderHighFlood: with high always waiting, one pick in ten still goes to the
// best non-high record, and only when one is waiting.
func TestMinimumShareUnderHighFlood(t *testing.T) {
	s := newScheduler(DefaultPolicy())
	for range 100 {
		push(s, domain.PriorityHigh, t0, "high")
	}
	push(s, domain.PriorityLow, t0, "low")
	push(s, domain.PriorityNormal, t0, "normal")

	var nonHigh []int
	for i := 1; i <= 30; i++ {
		if got := pickLabel(s, t0); got != "high" {
			nonHigh = append(nonHigh, i)
			if (i == 10 && got != "normal") || (i == 20 && got != "low") {
				t.Errorf("pick %d = %q", i, got)
			}
		}
	}
	if len(nonHigh) != 2 || nonHigh[0] != 10 || nonHigh[1] != 20 {
		t.Errorf("non-high picks at %v, want [10 20]; pick 30 has nothing non-high left", nonHigh)
	}
}

func TestRemove(t *testing.T) {
	s := newScheduler(DefaultPolicy())
	for i, l := range []string{"keep1", "drop1", "keep2", "drop2"} {
		push(s, domain.PriorityNormal, t0.Add(time.Duration(i)*time.Second), l)
	}
	removed := s.remove(func(it *item) bool { return it.rec.correlationID[:4] == "drop" })
	if len(removed) != 2 || s.len(domain.PriorityNormal) != 2 {
		t.Fatalf("removed %d, left %d", len(removed), s.len(domain.PriorityNormal))
	}
	if a, b := pickLabel(s, t0), pickLabel(s, t0); a != "keep1" || b != "keep2" {
		t.Errorf("heap order after remove: %s, %s", a, b)
	}
}

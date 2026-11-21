package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// QueueDepth is the backlog held in PostgreSQL (FR-4.1).
type QueueDepth struct {
	// ByStatus counts the notifications in each non-terminal status. Every non-terminal status is
	// present, with 0 when there are none.
	ByStatus map[domain.Status]int
	// Due counts the pending and retrying notifications that are due now: the dispatcher's
	// backlog. Scheduled and backing-off notifications are not due yet.
	Due int
	// OldestDue is when the longest-waiting due notification became due, or nil if none is due.
	OldestDue *time.Time
}

// activeStatuses are the statuses a notification moves through before it is finished.
var activeStatuses = []domain.Status{
	domain.StatusPending, domain.StatusQueued, domain.StatusProcessing, domain.StatusRetrying,
}

// QueueDepth counts the unfinished notifications. Finished rows are not read: the status index
// holds a separate range per status, so the statement scans the backlog only.
func (s *Store) QueueDepth(ctx context.Context) (QueueDepth, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, count(*),
		       count(*) FILTER (WHERE status IN ('pending', 'retrying') AND next_attempt_at <= now()),
		       min(next_attempt_at) FILTER (WHERE status IN ('pending', 'retrying') AND next_attempt_at <= now())
		  FROM notifications
		 WHERE status = ANY($1)
		 GROUP BY status`, statusStrings(activeStatuses))
	if err != nil {
		return QueueDepth{}, fmt.Errorf("queue depth: %w", err)
	}
	defer rows.Close()

	d := QueueDepth{ByStatus: make(map[domain.Status]int, len(activeStatuses))}
	for _, st := range activeStatuses {
		d.ByStatus[st] = 0
	}
	for rows.Next() {
		var (
			status     string
			total, due int
			oldest     *time.Time
		)
		if err := rows.Scan(&status, &total, &due, &oldest); err != nil {
			return QueueDepth{}, fmt.Errorf("queue depth: %w", err)
		}
		d.ByStatus[domain.Status(status)] = total
		d.Due += due
		if oldest != nil && (d.OldestDue == nil || oldest.Before(*d.OldestDue)) {
			t := oldest.UTC()
			d.OldestDue = &t
		}
	}
	if err := rows.Err(); err != nil {
		return QueueDepth{}, fmt.Errorf("queue depth: %w", err)
	}
	return d, nil
}

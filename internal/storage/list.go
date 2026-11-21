package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// List limits.
const (
	DefaultListLimit = 50
	MaxListLimit     = 500
)

// ListFilter selects notifications for ListNotifications (FR-1.5). Zero values mean "no
// filter".
type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	BatchID *uuid.UUID
	// CreatedFrom and CreatedTo bound created_at as [CreatedFrom, CreatedTo).
	CreatedFrom *time.Time
	CreatedTo   *time.Time
	// After is the cursor: the id of the last notification of the previous page.
	After *uuid.UUID
	// Limit is the page size, clamped to [1, MaxListLimit]; 0 means DefaultListLimit.
	Limit int
}

// ListPage is one page of results.
type ListPage struct {
	Items []domain.Notification
	// Next is the cursor for the following page, or nil on the last page.
	Next *uuid.UUID
}

// ListNotifications returns notifications newest first with keyset pagination on id. Ids are
// UUIDv7, so id order is creation order, and every cursor maps to a created_at bound that
// prunes older partitions.
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) (ListPage, error) {
	limit := f.Limit
	switch {
	case limit == 0:
		limit = DefaultListLimit
	case limit < 1:
		limit = 1
	case limit > MaxListLimit:
		limit = MaxListLimit
	}

	var (
		conds []string
		args  []any
	)
	// where adds a condition; each %d in cond is replaced by the placeholder number of v.
	where := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != nil {
		where("status = $%d", string(*f.Status))
	}
	if f.Channel != nil {
		where("channel = $%d", string(*f.Channel))
	}
	if f.BatchID != nil {
		where("batch_id = $%d", *f.BatchID)
		// All items of a batch share the batch's created_at (see CreateBatch).
		where("created_at = $%d", domain.IDTime(*f.BatchID))
	}
	if f.CreatedFrom != nil {
		where("created_at >= $%d", *f.CreatedFrom)
	}
	if f.CreatedTo != nil {
		where("created_at < $%d", *f.CreatedTo)
	}
	if f.After != nil {
		where("id < $%d", *f.After)
		where("created_at <= $%d", domain.IDTime(*f.After))
	}

	sql := `SELECT ` + columns("") + ` FROM notifications`
	if len(conds) > 0 {
		sql += ` WHERE ` + strings.Join(conds, " AND ")
	}
	args = append(args, limit+1)
	sql += fmt.Sprintf(` ORDER BY id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return ListPage{}, fmt.Errorf("list notifications: %w", err)
	}
	items, err := collectNotifications(rows)
	if err != nil {
		return ListPage{}, fmt.Errorf("list notifications: %w", err)
	}
	page := ListPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		next := page.Items[limit-1].ID
		page.Next = &next
	}
	return page, nil
}

func collectNotifications(rows pgx.Rows) ([]domain.Notification, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Notification, error) {
		return scanNotification(r)
	})
}

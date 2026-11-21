package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// MaxBatchSize is the largest batch accepted by CreateBatch (FR-1.2).
const MaxBatchSize = 1000

// IdempotencyKey is a client-supplied Idempotency-Key and the SHA-256 hash of the canonicalised
// request it came with (ADR-0004).
type IdempotencyKey struct {
	Key         string
	RequestHash [32]byte
}

// NewNotification is the input for creating one notification. Content validation (FR-2.4)
// happens before the storage layer.
type NewNotification struct {
	Channel   domain.Channel
	Priority  domain.Priority
	Recipient string
	Content   string
	// ScheduledAt delays the first delivery attempt (B-2). Nil means as soon as possible.
	ScheduledAt *time.Time
	// IdempotencyKey is optional. In a batch it is the item's own key.
	IdempotencyKey *IdempotencyKey
}

// CreateNotification stores a pending notification and its first history row, and wakes the
// dispatcher if it is due now (FR-1.1, ADR-0003).
//
// With an idempotency key that was already used for the same request, it returns the original
// notification and created=false. If the key was used for a different request, it returns
// domain.ErrIdempotencyKeyReused.
func (s *Store) CreateNotification(ctx context.Context, in NewNotification, correlationID string) (n domain.Notification, created bool, err error) {
	id := domain.NewIDAt(s.now())
	createdAt := domain.IDTime(id)

	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if in.IdempotencyKey != nil {
			existing, err := s.claimKeys(ctx, tx, []keyClaim{{*in.IdempotencyKey, resourceNotification, id, createdAt}})
			if err != nil {
				return err
			}
			if prior, ok := existing[in.IdempotencyKey.Key]; ok {
				if !prior.matches(*in.IdempotencyKey, resourceNotification) {
					return domain.ErrIdempotencyKeyReused
				}
				n, err = getNotification(ctx, tx, prior.resourceID)
				return err
			}
		}
		rows, err := insertNotifications(ctx, tx, []uuid.UUID{id}, createdAt, nil, []NewNotification{in}, correlationID)
		if err != nil {
			return err
		}
		n, created = rows[0], true
		if isDueNow(in, s.now()) {
			return notifyDue(ctx, tx)
		}
		return nil
	})
	if err != nil {
		return domain.Notification{}, false, err
	}
	return n, created, nil
}

// NewBatch is the input for CreateBatch.
type NewBatch struct {
	Items []NewNotification
	// IdempotencyKey is the batch-level key (the Idempotency-Key header). Optional.
	IdempotencyKey *IdempotencyKey
}

// ItemError attributes a batch failure to one item of the request.
type ItemError struct {
	Index int
	Err   error
}

func (e *ItemError) Error() string { return fmt.Sprintf("item %d: %v", e.Index, e.Err) }

// Unwrap exposes the cause, e.g. domain.ErrIdempotencyKeyReused.
func (e *ItemError) Unwrap() error { return e.Err }

// BatchItem is one item of a batch result, in request order.
type BatchItem struct {
	Notification domain.Notification
	// Duplicate is true if the item's own idempotency key matched an earlier request, so the
	// existing notification is returned and nothing new was created for this item. Duplicates
	// do not belong to the new batch and are not counted in Batch.Total.
	Duplicate bool
}

// BatchResult is the outcome of CreateBatch.
type BatchResult struct {
	Batch domain.Batch
	Items []BatchItem
	// Created is false if the batch-level idempotency key matched an earlier request. Batch
	// and Items then reproduce the original result, in request order and with duplicates
	// marked, showing the notifications' current state.
	Created bool
}

// CreateBatch stores up to MaxBatchSize notifications in one transaction with one multi-row
// INSERT (FR-1.2). All items share the batch's created_at, so they land in one partition and
// batch queries are pruned to it; their ids sort in request order.
func (s *Store) CreateBatch(ctx context.Context, in NewBatch, correlationID string) (BatchResult, error) {
	if err := validateBatch(in); err != nil {
		return BatchResult{}, err
	}
	batchID := domain.NewIDAt(s.now())
	createdAt := domain.IDTime(batchID)

	var res BatchResult
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if in.IdempotencyKey != nil {
			existing, err := s.claimKeys(ctx, tx, []keyClaim{{*in.IdempotencyKey, resourceBatch, batchID, createdAt}})
			if err != nil {
				return err
			}
			if prior, ok := existing[in.IdempotencyKey.Key]; ok {
				if !prior.matches(*in.IdempotencyKey, resourceBatch) {
					return domain.ErrIdempotencyKeyReused
				}
				res, err = loadBatch(ctx, tx, prior.resourceID, in.Items)
				return err
			}
		}

		// Claim item keys; items whose key is already live are duplicates.
		ids := make([]uuid.UUID, len(in.Items))
		var claims []keyClaim
		for i, item := range in.Items {
			ids[i] = domain.NewBatchItemID(createdAt, i)
			if item.IdempotencyKey != nil {
				claims = append(claims, keyClaim{*item.IdempotencyKey, resourceNotification, ids[i], createdAt})
			}
		}
		existing, err := s.claimKeys(ctx, tx, claims)
		if err != nil {
			return err
		}

		res = BatchResult{Items: make([]BatchItem, len(in.Items)), Created: true}
		var newIDs []uuid.UUID
		var newItems []NewNotification
		newIndex := make([]int, 0, len(in.Items))
		dueNow := false
		for i, item := range in.Items {
			if item.IdempotencyKey != nil {
				if prior, ok := existing[item.IdempotencyKey.Key]; ok {
					if !prior.matches(*item.IdempotencyKey, resourceNotification) {
						return &ItemError{Index: i, Err: domain.ErrIdempotencyKeyReused}
					}
					n, err := getNotification(ctx, tx, prior.resourceID)
					if err != nil {
						return fmt.Errorf("item %d: load duplicate: %w", i, err)
					}
					res.Items[i] = BatchItem{Notification: n, Duplicate: true}
					continue
				}
			}
			newIDs, newItems, newIndex = append(newIDs, ids[i]), append(newItems, item), append(newIndex, i)
			dueNow = dueNow || isDueNow(item, s.now())
		}

		res.Batch = domain.Batch{ID: batchID, CreatedAt: createdAt, Total: len(newItems), CorrelationID: correlationID}
		if _, err := tx.Exec(ctx,
			`INSERT INTO batches (id, created_at, total, correlation_id) VALUES ($1, $2, $3, $4)`,
			batchID, createdAt, len(newItems), correlationID); err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}
		if len(newItems) > 0 {
			rows, err := insertNotifications(ctx, tx, newIDs, createdAt, &batchID, newItems, correlationID)
			if err != nil {
				return err
			}
			for k, n := range rows {
				res.Items[newIndex[k]] = BatchItem{Notification: n}
			}
			res.Batch.StatusCounts = map[domain.Status]int{domain.StatusPending: len(rows)}
		}
		if dueNow {
			return notifyDue(ctx, tx)
		}
		return nil
	})
	if err != nil {
		return BatchResult{}, err
	}
	return res, nil
}

func validateBatch(in NewBatch) error {
	if len(in.Items) == 0 || len(in.Items) > MaxBatchSize {
		return fmt.Errorf("%w: a batch must contain 1 to %d items, got %d",
			domain.ErrInvalidArgument, MaxBatchSize, len(in.Items))
	}
	seen := make(map[string]int)
	if in.IdempotencyKey != nil {
		seen[in.IdempotencyKey.Key] = -1
	}
	for i, item := range in.Items {
		if item.IdempotencyKey == nil {
			continue
		}
		if j, dup := seen[item.IdempotencyKey.Key]; dup {
			if j < 0 {
				return fmt.Errorf("%w: item %d reuses the batch idempotency key", domain.ErrInvalidArgument, i)
			}
			return fmt.Errorf("%w: items %d and %d share an idempotency key", domain.ErrInvalidArgument, j, i)
		}
		seen[item.IdempotencyKey.Key] = i
	}
	return nil
}

// insertNotifications inserts rows (all with the same created_at) and their "created" history
// rows in one statement, and returns them in the order of ids.
func insertNotifications(ctx context.Context, q querier, ids []uuid.UUID, createdAt time.Time, batchID *uuid.UUID, items []NewNotification, correlationID string) ([]domain.Notification, error) {
	n := len(items)
	channels, priorities := make([]string, n), make([]string, n)
	recipients, contents := make([]string, n), make([]string, n)
	scheduled := make([]*time.Time, n)
	for i, it := range items {
		channels[i], priorities[i] = string(it.Channel), string(it.Priority)
		recipients[i], contents[i], scheduled[i] = it.Recipient, it.Content, it.ScheduledAt
	}

	rows, err := q.Query(ctx, `
WITH ins AS (
    INSERT INTO notifications
           (id, created_at, batch_id, channel, priority, recipient, content,
            next_attempt_at, scheduled_at, correlation_id)
    SELECT u.id, $2, $8, u.channel, u.priority, u.recipient, u.content,
           COALESCE(u.scheduled_at, now()), u.scheduled_at, $9
      FROM unnest($1::uuid[], $3::text[], $4::text[], $5::text[], $6::text[], $7::timestamptz[])
           AS u(id, channel, priority, recipient, content, scheduled_at)
 RETURNING `+columns("")+`
), hist AS (
    INSERT INTO notification_status_history
           (notification_id, notification_created_at, from_status, to_status, attempt, reason)
    SELECT id, created_at, NULL, status, attempts, 'created' FROM ins
)
SELECT `+columns("")+` FROM ins`,
		ids, createdAt, channels, priorities, recipients, contents, scheduled, batchID, correlationID)
	if err != nil {
		return nil, fmt.Errorf("insert notifications: %w", err)
	}
	inserted, err := collectNotifications(rows)
	if err != nil {
		return nil, fmt.Errorf("insert notifications: %w", err)
	}
	// RETURNING order is unspecified; restore the order of ids.
	byID := make(map[uuid.UUID]domain.Notification, len(inserted))
	for _, row := range inserted {
		byID[row.ID] = row
	}
	out := make([]domain.Notification, len(ids))
	for i, id := range ids {
		row, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("insert notifications: row %s missing from RETURNING", id)
		}
		out[i] = row
	}
	return out, nil
}

func isDueNow(in NewNotification, now time.Time) bool {
	return in.ScheduledAt == nil || !in.ScheduledAt.After(now)
}

// GetBatch returns a batch with the number of its notifications in each status (FR-1.3).
func (s *Store) GetBatch(ctx context.Context, id uuid.UUID) (domain.Batch, error) {
	return getBatch(ctx, s.pool, id)
}

func getBatch(ctx context.Context, q querier, id uuid.UUID) (domain.Batch, error) {
	if !domain.IsV7(id) {
		return domain.Batch{}, domain.ErrNotFound
	}
	b := domain.Batch{StatusCounts: map[domain.Status]int{}}
	err := q.QueryRow(ctx,
		`SELECT id, created_at, total, correlation_id FROM batches WHERE id = $1`, id,
	).Scan(&b.ID, &b.CreatedAt, &b.Total, &b.CorrelationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Batch{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Batch{}, fmt.Errorf("get batch: %w", err)
	}
	b.CreatedAt = b.CreatedAt.UTC()

	rows, err := q.Query(ctx, `
		SELECT status, count(*) FROM notifications
		 WHERE batch_id = $1 AND created_at = $2
		 GROUP BY status`, id, b.CreatedAt)
	if err != nil {
		return domain.Batch{}, fmt.Errorf("count batch statuses: %w", err)
	}
	var (
		status string
		count  int
	)
	_, err = pgx.ForEachRow(rows, []any{&status, &count}, func() error {
		b.StatusCounts[domain.Status(status)] = count
		return nil
	})
	if err != nil {
		return domain.Batch{}, fmt.Errorf("count batch statuses: %w", err)
	}
	return b, nil
}

// loadBatch rebuilds the result of an earlier CreateBatch call for a replay of the same request
// (same batch key and hash, so items is the same list). Created items are placed by the request
// index stored in their ids (domain.BatchSeq); the remaining positions were duplicates of
// earlier requests and are found through their item keys.
func loadBatch(ctx context.Context, q querier, id uuid.UUID, items []NewNotification) (BatchResult, error) {
	b, err := getBatch(ctx, q, id)
	if err != nil {
		return BatchResult{}, err
	}
	rows, err := q.Query(ctx, `SELECT `+columns("")+` FROM notifications
		WHERE batch_id = $1 AND created_at = $2 ORDER BY id`, id, b.CreatedAt)
	if err != nil {
		return BatchResult{}, fmt.Errorf("load batch items: %w", err)
	}
	created, err := collectNotifications(rows)
	if err != nil {
		return BatchResult{}, fmt.Errorf("load batch items: %w", err)
	}

	res := BatchResult{Batch: b, Items: make([]BatchItem, len(items))}
	filled := make([]bool, len(items))
	for _, n := range created {
		i := domain.BatchSeq(n.ID)
		if i >= len(items) {
			return BatchResult{}, fmt.Errorf("load batch items: item index %d outside the %d-item request", i, len(items))
		}
		res.Items[i], filled[i] = BatchItem{Notification: n}, true
	}
	for i, item := range items {
		if filled[i] {
			continue
		}
		if item.IdempotencyKey == nil {
			return BatchResult{}, fmt.Errorf("load batch items: item %d was neither created nor deduplicated", i)
		}
		var nid uuid.UUID
		if err := q.QueryRow(ctx, `SELECT resource_id FROM idempotency_keys WHERE key = $1`,
			item.IdempotencyKey.Key).Scan(&nid); err != nil {
			return BatchResult{}, fmt.Errorf("load batch item %d: resolve its idempotency key: %w", i, err)
		}
		n, err := getNotification(ctx, q, nid)
		if err != nil {
			return BatchResult{}, fmt.Errorf("load batch item %d: %w", i, err)
		}
		res.Items[i] = BatchItem{Notification: n, Duplicate: true}
	}
	return res, nil
}

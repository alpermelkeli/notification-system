package api

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// fakeStore is an in-memory Store. It implements idempotency like the real one so handler tests
// can check status codes and headers; storage semantics are tested in internal/storage.
type fakeStore struct {
	mu      sync.Mutex
	byID    map[uuid.UUID]domain.Notification
	keys    map[string]fakeKey
	batches map[uuid.UUID]storage.BatchResult
	err     error // returned by every method when set
	block   bool  // CreateNotification waits until its context ends

	lastCreate      storage.NewNotification
	lastBatch       storage.NewBatch
	lastFilter      storage.ListFilter
	lastCorrelation string
	lastOrigin      storage.Origin
	listPage        storage.ListPage
}

type fakeKey struct {
	hash [32]byte
	id   uuid.UUID
}

func newFakeStore() *fakeStore {
	return &fakeStore{byID: map[uuid.UUID]domain.Notification{}, keys: map[string]fakeKey{}, batches: map[uuid.UUID]storage.BatchResult{}}
}

func (f *fakeStore) newNotification(in storage.NewNotification, correlationID string, batchID *uuid.UUID) domain.Notification {
	now := time.Now().UTC()
	id := domain.NewIDAt(now)
	next := now
	if in.ScheduledAt != nil {
		next = *in.ScheduledAt
	}
	n := domain.Notification{
		ID: id, CreatedAt: domain.IDTime(id), BatchID: batchID, Channel: in.Channel, Priority: in.Priority,
		Recipient: in.Recipient, Content: in.Content, Status: domain.StatusPending, NextAttemptAt: next,
		ScheduledAt: in.ScheduledAt, CorrelationID: correlationID, UpdatedAt: now,
	}
	f.byID[id] = n
	return n
}

func (f *fakeStore) CreateNotification(ctx context.Context, in storage.NewNotification, origin storage.Origin) (domain.Notification, bool, error) {
	correlationID := origin.CorrelationID
	f.lastOrigin = origin
	if f.block {
		<-ctx.Done()
		return domain.Notification{}, false, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return domain.Notification{}, false, f.err
	}
	f.lastCreate, f.lastCorrelation = in, correlationID
	if k := in.IdempotencyKey; k != nil {
		if prior, ok := f.keys[k.Key]; ok {
			if prior.hash != k.RequestHash {
				return domain.Notification{}, false, domain.ErrIdempotencyKeyReused
			}
			return f.byID[prior.id], false, nil
		}
	}
	n := f.newNotification(in, correlationID, nil)
	if k := in.IdempotencyKey; k != nil {
		f.keys[k.Key] = fakeKey{k.RequestHash, n.ID}
	}
	return n, true, nil
}

func (f *fakeStore) CreateBatch(_ context.Context, in storage.NewBatch, origin storage.Origin) (storage.BatchResult, error) {
	correlationID := origin.CorrelationID
	f.lastOrigin = origin
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return storage.BatchResult{}, f.err
	}
	f.lastBatch, f.lastCorrelation = in, correlationID
	if k := in.IdempotencyKey; k != nil {
		if prior, ok := f.keys[k.Key]; ok {
			if prior.hash != k.RequestHash {
				return storage.BatchResult{}, domain.ErrIdempotencyKeyReused
			}
			res := f.batches[prior.id]
			res.Created = false
			return res, nil
		}
	}
	batchID := domain.NewIDAt(time.Now())
	res := storage.BatchResult{Created: true, Batch: domain.Batch{
		ID: batchID, CreatedAt: domain.IDTime(batchID), Total: len(in.Items), CorrelationID: correlationID,
		StatusCounts: map[domain.Status]int{domain.StatusPending: len(in.Items)},
	}}
	for _, item := range in.Items {
		res.Items = append(res.Items, storage.BatchItem{Notification: f.newNotification(item, correlationID, &batchID)})
	}
	f.batches[batchID] = res
	if k := in.IdempotencyKey; k != nil {
		f.keys[k.Key] = fakeKey{k.RequestHash, batchID}
	}
	return res, nil
}

func (f *fakeStore) get(id uuid.UUID) (domain.Notification, error) {
	if f.err != nil {
		return domain.Notification{}, f.err
	}
	n, ok := f.byID[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return n, nil
}

func (f *fakeStore) GetNotification(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.get(id)
}

func (f *fakeStore) History(_ context.Context, id uuid.UUID) ([]domain.StatusChange, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.get(id)
	if err != nil {
		return nil, err
	}
	return []domain.StatusChange{{To: domain.StatusPending, Reason: "created", At: n.CreatedAt}}, nil
}

func (f *fakeStore) transition(id uuid.UUID, from []domain.Status, to domain.Status) (domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.get(id)
	if err != nil {
		return n, err
	}
	for _, s := range from {
		if n.Status == s {
			n.Status = to
			f.byID[id] = n
			return n, nil
		}
	}
	return domain.Notification{}, &domain.TransitionError{Current: n.Status, Target: to}
}

func (f *fakeStore) Cancel(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	return f.transition(id, domain.CancellableStatuses, domain.StatusCancelled)
}

func (f *fakeStore) Replay(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	return f.transition(id, []domain.Status{domain.StatusFailed}, domain.StatusPending)
}

func (f *fakeStore) ListNotifications(_ context.Context, filter storage.ListFilter) (storage.ListPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastFilter = filter
	return f.listPage, f.err
}

func (f *fakeStore) GetBatch(_ context.Context, id uuid.UUID) (domain.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return domain.Batch{}, f.err
	}
	res, ok := f.batches[id]
	if !ok {
		return domain.Batch{}, domain.ErrNotFound
	}
	return res.Batch, nil
}

func (f *fakeStore) setStatus(id uuid.UUID, s domain.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.byID[id]
	n.Status = s
	f.byID[id] = n
}

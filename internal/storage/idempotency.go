package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	resourceNotification = "notification"
	resourceBatch        = "batch"
)

// errKeyRace is a defensive check: a key that conflicted on insert must still exist when read
// back, because ON CONFLICT DO UPDATE keeps the conflicting row locked until commit.
var errKeyRace = errors.New("idempotency key changed concurrently, retry the request")

type keyClaim struct {
	key          IdempotencyKey
	resourceType string
	resourceID   uuid.UUID
	createdAt    time.Time
}

type priorKey struct {
	requestHash  []byte
	resourceType string
	resourceID   uuid.UUID
}

func (p priorKey) matches(k IdempotencyKey, resourceType string) bool {
	return p.resourceType == resourceType && bytes.Equal(p.requestHash, k.RequestHash[:])
}

// claimKeys records the keys for resources about to be created in tx. It returns the earlier
// record of every key that is still live; those resources must not be created again.
//
// Concurrent requests with the same key serialise on the primary key: the second INSERT waits
// for the first transaction and then sees its committed row (ADR-0004). An expired key that the
// cleanup has not deleted yet is taken over.
func (s *Store) claimKeys(ctx context.Context, tx pgx.Tx, claims []keyClaim) (map[string]priorKey, error) {
	if len(claims) == 0 {
		return nil, nil
	}
	// Insert in key order: two batches claiming the same keys in different orders would
	// otherwise deadlock on each other's speculative inserts.
	claims = slices.SortedFunc(slices.Values(claims), func(a, b keyClaim) int {
		return strings.Compare(a.key.Key, b.key.Key)
	})
	keys, hashes := make([]string, len(claims)), make([][]byte, len(claims))
	types, ids, createdAts := make([]string, len(claims)), make([]uuid.UUID, len(claims)), make([]time.Time, len(claims))
	for i, c := range claims {
		keys[i], hashes[i] = c.key.Key, c.key.RequestHash[:]
		types[i], ids[i], createdAts[i] = c.resourceType, c.resourceID, c.createdAt
	}

	rows, err := tx.Query(ctx, `
		INSERT INTO idempotency_keys
		       (key, request_hash, resource_type, resource_id, resource_created_at, expires_at)
		SELECT u.*, now() + make_interval(secs => $6)
		  FROM unnest($1::text[], $2::bytea[], $3::text[], $4::uuid[], $5::timestamptz[]) AS u
		ON CONFLICT (key) DO UPDATE
		   SET request_hash = EXCLUDED.request_hash, resource_type = EXCLUDED.resource_type,
		       resource_id = EXCLUDED.resource_id, resource_created_at = EXCLUDED.resource_created_at,
		       created_at = now(), expires_at = EXCLUDED.expires_at
		 WHERE idempotency_keys.expires_at <= now()
		RETURNING key`, keys, hashes, types, ids, createdAts, s.ttl.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim idempotency keys: %w", err)
	}
	claimed, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("claim idempotency keys: %w", err)
	}
	if len(claimed) == len(claims) {
		return nil, nil
	}

	isClaimed := make(map[string]bool, len(claimed))
	for _, k := range claimed {
		isClaimed[k] = true
	}
	var taken []string
	for _, k := range keys {
		if !isClaimed[k] {
			taken = append(taken, k)
		}
	}
	rows, err = tx.Query(ctx, `
		SELECT key, request_hash, resource_type, resource_id
		  FROM idempotency_keys WHERE key = ANY($1)`, taken)
	if err != nil {
		return nil, fmt.Errorf("read idempotency keys: %w", err)
	}
	prior := make(map[string]priorKey, len(taken))
	var (
		key string
		p   priorKey
	)
	if _, err := pgx.ForEachRow(rows, []any{&key, &p.requestHash, &p.resourceType, &p.resourceID}, func() error {
		prior[key] = priorKey{requestHash: bytes.Clone(p.requestHash), resourceType: p.resourceType, resourceID: p.resourceID}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read idempotency keys: %w", err)
	}
	if len(prior) != len(taken) {
		return nil, errKeyRace
	}
	return prior, nil
}

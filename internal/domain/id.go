package domain

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewIDAt returns a UUIDv7 (RFC 9562) whose 48-bit timestamp is t in milliseconds, followed by
// 74 random bits. Use IDTime to recover the timestamp.
//
// The storage layer partitions notifications by created_at and sets created_at to IDTime(id),
// so a lookup by id alone can be pruned to one partition.
func NewIDAt(t time.Time) uuid.UUID {
	return newV7(t)
}

// MaxBatchSeq is the largest sequence number NewBatchItemID accepts (12 bits).
const MaxBatchSeq = 1<<12 - 1

// NewBatchItemID returns a UUIDv7 for the seq-th item of a batch created at t. It stores seq in
// the 12-bit rand_a field (the "fixed-length dedicated counter" of RFC 9562, section 6.2), so
// all items of a batch share the batch's timestamp and partition, and sorting them by id
// restores request order. seq must be in [0, MaxBatchSeq].
func NewBatchItemID(t time.Time, seq int) uuid.UUID {
	if seq < 0 || seq > MaxBatchSeq {
		panic(fmt.Sprintf("domain: batch sequence %d out of range", seq))
	}
	u := newV7(t)
	u[6] = 0x70 | byte(seq>>8) //nolint:gosec // G115: seq < 4096
	u[7] = byte(seq)           //nolint:gosec // G115: low byte
	return u
}

func newV7(t time.Time) uuid.UUID {
	var u uuid.UUID
	_, _ = rand.Read(u[6:]) // crypto/rand.Read never returns an error (Go 1.24+)

	var ms [8]byte
	binary.BigEndian.PutUint64(ms[:], uint64(t.UnixMilli())) //nolint:gosec // G115: post-1970 timestamps are positive
	copy(u[:6], ms[2:])

	u[6] = 0x70 | (u[6] & 0x0f) // version 7
	u[8] = 0x80 | (u[8] & 0x3f) // RFC 9562 variant
	return u
}

// IDTime returns the millisecond timestamp embedded in a UUIDv7, in UTC.
func IDTime(id uuid.UUID) time.Time {
	var ms [8]byte
	copy(ms[2:], id[:6])
	return time.UnixMilli(int64(binary.BigEndian.Uint64(ms[:]))).UTC() //nolint:gosec // G115: 48-bit value fits
}

// IsV7 reports whether id is a version-7 UUID. Ids of other versions cannot have been issued by
// this system, so lookups can reject them early.
func IsV7(id uuid.UUID) bool {
	return id.Version() == 7 && id.Variant() == uuid.RFC4122
}

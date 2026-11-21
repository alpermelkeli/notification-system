package api

import (
	"encoding/base64"
	"time"

	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// createRequest is the body of POST /v1/notifications (schema CreateNotificationRequest).
type createRequest struct {
	Recipient   string     `json:"recipient"`
	Channel     string     `json:"channel"`
	Content     string     `json:"content"`
	Priority    string     `json:"priority,omitempty"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
}

// batchItemRequest is one item of POST /v1/notifications/batch (schema BatchItemRequest).
type batchItemRequest struct {
	createRequest
	IdempotencyKey *string `json:"idempotencyKey,omitempty"`
}

// batchRequest is the body of POST /v1/notifications/batch (schema CreateBatchRequest).
type batchRequest struct {
	Notifications []batchItemRequest `json:"notifications"`
}

// notificationJSON is schema Notification.
type notificationJSON struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batchId,omitempty"`
	Channel           string     `json:"channel"`
	Priority          string     `json:"priority"`
	Recipient         string     `json:"recipient"`
	Content           string     `json:"content"`
	Status            string     `json:"status"`
	Attempts          int        `json:"attempts"`
	NextAttemptAt     time.Time  `json:"nextAttemptAt"`
	ScheduledAt       *time.Time `json:"scheduledAt,omitempty"`
	QueuedAt          *time.Time `json:"queuedAt,omitempty"`
	SentAt            *time.Time `json:"sentAt,omitempty"`
	ProviderMessageID *string    `json:"providerMessageId,omitempty"`
	LastError         *string    `json:"lastError,omitempty"`
	CorrelationID     string     `json:"correlationId,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
}

func toNotificationJSON(n domain.Notification) notificationJSON {
	return notificationJSON{
		ID: n.ID, BatchID: n.BatchID, Channel: string(n.Channel), Priority: string(n.Priority),
		Recipient: n.Recipient, Content: n.Content, Status: string(n.Status), Attempts: n.Attempts,
		NextAttemptAt: n.NextAttemptAt, ScheduledAt: n.ScheduledAt, QueuedAt: n.QueuedAt, SentAt: n.SentAt,
		ProviderMessageID: n.ProviderMessageID, LastError: n.LastError, CorrelationID: n.CorrelationID,
		CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt,
	}
}

// listJSON is schema NotificationList.
type listJSON struct {
	Items      []notificationJSON `json:"items"`
	NextCursor string             `json:"nextCursor,omitempty"`
}

// statusChangeJSON is schema StatusChange.
type statusChangeJSON struct {
	From    *string   `json:"from,omitempty"`
	To      string    `json:"to"`
	Attempt int       `json:"attempt"`
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"at"`
}

// historyJSON is schema History.
type historyJSON struct {
	ID      uuid.UUID          `json:"id"`
	Changes []statusChangeJSON `json:"changes"`
}

func toHistoryJSON(id uuid.UUID, changes []domain.StatusChange) historyJSON {
	out := historyJSON{ID: id, Changes: make([]statusChangeJSON, len(changes))}
	for i, c := range changes {
		var from *string
		if c.From != nil {
			f := string(*c.From)
			from = &f
		}
		out.Changes[i] = statusChangeJSON{From: from, To: string(c.To), Attempt: c.Attempt, Reason: c.Reason, At: c.At}
	}
	return out
}

// batchJSON is schema Batch.
type batchJSON struct {
	ID            uuid.UUID      `json:"id"`
	CreatedAt     time.Time      `json:"createdAt"`
	Total         int            `json:"total"`
	CorrelationID string         `json:"correlationId,omitempty"`
	StatusCounts  map[string]int `json:"statusCounts"`
}

func toBatchJSON(b domain.Batch) batchJSON {
	counts := make(map[string]int, len(b.StatusCounts))
	for s, n := range b.StatusCounts {
		counts[string(s)] = n
	}
	return batchJSON{ID: b.ID, CreatedAt: b.CreatedAt, Total: b.Total, CorrelationID: b.CorrelationID, StatusCounts: counts}
}

// batchItemJSON is schema BatchItemResult.
type batchItemJSON struct {
	Index        int              `json:"index"`
	Duplicate    bool             `json:"duplicate"`
	Notification notificationJSON `json:"notification"`
}

// batchCreatedJSON is schema BatchCreated.
type batchCreatedJSON struct {
	Batch batchJSON       `json:"batch"`
	Items []batchItemJSON `json:"items"`
}

func toBatchCreatedJSON(res storage.BatchResult) batchCreatedJSON {
	out := batchCreatedJSON{Batch: toBatchJSON(res.Batch), Items: make([]batchItemJSON, len(res.Items))}
	for i, it := range res.Items {
		out.Items[i] = batchItemJSON{Index: i, Duplicate: it.Duplicate, Notification: toNotificationJSON(it.Notification)}
	}
	return out
}

// Cursors are the id of the last item of a page, base64url-encoded. They are opaque to clients
// so the pagination scheme can change without breaking them.
func encodeCursor(id uuid.UUID) string { return base64.RawURLEncoding.EncodeToString(id[:]) }

func decodeCursor(s string) (uuid.UUID, bool) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != len(uuid.UUID{}) {
		return uuid.UUID{}, false
	}
	id := uuid.UUID(b)
	return id, domain.IsV7(id)
}

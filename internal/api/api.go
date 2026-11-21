// Package api implements the public HTTP API (FR-1.x) described by api/openapi.yaml. Handlers
// decode and validate requests, call the store and map domain errors to HTTP responses; they
// never talk to Kafka (ADR-0003).
package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	apispec "github.com/alpermelkeli/notification-system/api"
	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

// Store is the persistence the API needs. *storage.Store implements it.
type Store interface {
	CreateNotification(ctx context.Context, in storage.NewNotification, correlationID string) (domain.Notification, bool, error)
	CreateBatch(ctx context.Context, in storage.NewBatch, correlationID string) (storage.BatchResult, error)
	GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	History(ctx context.Context, id uuid.UUID) ([]domain.StatusChange, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Replay(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	ListNotifications(ctx context.Context, f storage.ListFilter) (storage.ListPage, error)
	GetBatch(ctx context.Context, id uuid.UUID) (domain.Batch, error)
}

// Options configures the API.
type Options struct {
	// MaxScheduleAhead bounds scheduledAt (B-2). It must stay below the data retention, or a
	// scheduled notification would be dropped with its partition before delivery.
	MaxScheduleAhead time.Duration
	// RequestTimeout bounds each API request. Default 10s.
	RequestTimeout time.Duration
	// Now returns the current time. Default time.Now.
	Now func() time.Time
}

// Request body limits. A batch of 1000 SMS fits comfortably in 10 MiB; nginx allows slightly
// more so that the API, not the load balancer, produces the error envelope.
const (
	maxSingleBody = 256 << 10
	maxBatchBody  = 10 << 20
)

// Response headers specific to this API.
const (
	headerIdempotencyKey    = "Idempotency-Key"
	headerIdempotentReplay  = "Idempotent-Replayed"
	maxIdempotencyKeyLength = 255
)

// Handler serves the API.
type Handler struct {
	store  Store
	logger *slog.Logger
	opts   Options
}

// New returns a Handler.
func New(store Store, logger *slog.Logger, opts Options) *Handler {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 10 * time.Second
	}
	if opts.MaxScheduleAhead <= 0 {
		opts.MaxScheduleAhead = 7 * 24 * time.Hour
	}
	return &Handler{store: store, logger: logger, opts: opts}
}

// Routes mounts the API and its documentation on r.
func (h *Handler) Routes(r chi.Router) {
	r.Route("/v1", func(r chi.Router) {
		r.Use(h.deadline)
		r.Post("/notifications", h.createNotification)
		r.Get("/notifications", h.listNotifications)
		r.Post("/notifications/batch", h.createBatch)
		r.Get("/notifications/{id}", h.getNotification)
		r.Get("/notifications/{id}/history", h.getHistory)
		r.Post("/notifications/{id}/cancel", h.cancelNotification)
		r.Post("/notifications/{id}/retry", h.retryNotification)
		r.Get("/batches/{id}", h.getBatch)
	})
	r.Get("/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(apispec.OpenAPI)
	})
	r.Get("/docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", docsCSP)
		_, _ = w.Write([]byte(swaggerUI))
	})
}

// deadline bounds every API request, so a stalled database or a lock wait cannot hold a request
// and its pool connection indefinitely (the server sets no WriteTimeout because of WebSockets).
func (h *Handler) deadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), h.opts.RequestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Swagger UI renders /openapi.yaml from a pinned CDN build, so the binary does not embed the UI
// assets. Subresource Integrity pins the exact files, and the CSP allows nothing else to run.
const (
	swaggerInit = `window.onload = () => { window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" }); };`
	swaggerUI   = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Notification System API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css"
        integrity="sha384-wxLW6kwyHktdDGr6Pv1zgm/VGJh99lfUbzSn6HNHBENZlCN7W602k9VkGdxuFvPn" crossorigin="anonymous">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"
          integrity="sha384-wmyclcVGX/WhUkdkATwhaK1X1JtiNrr2EoYJ+diV3vj4v6OC5yCeSu+yW13SYJep" crossorigin="anonymous"></script>
  <script>` + swaggerInit + `</script>
</body>
</html>
`
)

var docsCSP = func() string {
	sum := sha256.Sum256([]byte(swaggerInit))
	return "default-src 'none'; " +
		"script-src https://unpkg.com 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'; " +
		"style-src https://unpkg.com 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; " +
		"base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
}()

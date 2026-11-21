// Package health implements liveness and readiness endpoints (FR-4.3).
//
//   - GET /healthz (liveness): the process is up and serving. It never checks dependencies, so
//     an outage of PostgreSQL or Kafka does not make an orchestrator restart healthy replicas.
//   - GET /readyz (readiness): every dependency answered within the timeout. A load balancer
//     or orchestrator stops routing traffic to an instance that is not ready.
package health

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/alpermelkeli/notification-system/internal/httpx"
)

// Check probes one dependency.
type Check struct {
	Name  string
	Probe func(ctx context.Context) error
}

// Response is the JSON body of both endpoints.
type Response struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

const (
	statusOK          = "ok"
	statusUnavailable = "unavailable"
)

// Liveness always answers 200.
func Liveness() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, Response{Status: statusOK})
	}
}

// Readiness runs all checks concurrently, each bounded by timeout, and answers 200 only if all
// of them pass, 503 otherwise. The response only says which check failed; the error itself (which
// can contain hosts and user names) is logged, never returned.
func Readiness(logger *slog.Logger, timeout time.Duration, checks ...Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		results := make(map[string]string, len(checks))
		var (
			mu sync.Mutex
			wg sync.WaitGroup
		)
		for _, c := range checks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := statusOK
				if err := c.Probe(ctx); err != nil {
					res = statusUnavailable
					logger.WarnContext(ctx, "readiness check failed", "check", c.Name, "err", err)
				}
				mu.Lock()
				results[c.Name] = res
				mu.Unlock()
			}()
		}
		wg.Wait()

		resp := Response{Status: statusOK, Checks: results}
		code := http.StatusOK
		for _, res := range results {
			if res != statusOK {
				resp.Status = statusUnavailable
				code = http.StatusServiceUnavailable
				break
			}
		}
		httpx.WriteJSON(w, code, resp)
	}
}

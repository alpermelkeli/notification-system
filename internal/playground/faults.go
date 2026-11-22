package playground

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/alpermelkeli/notification-system/internal/httpx"
)

// maxFaultsBody bounds the body forwarded to the mock provider and its answer.
const maxFaultsBody = 4 << 10

// handleFaults forwards GET and PUT to the mock provider's /admin/faults, which validates the
// rates. The browser cannot reach the mock provider itself: compose publishes only the load
// balancer.
//
// Upstream failures are answered with 500 or 504, never 502 or 503: nginx treats those as a
// failed API replica (proxy_next_upstream) and would take every replica out of rotation for a
// problem of the mock provider.
func (h *Handler) handleFaults(w http.ResponseWriter, r *http.Request) {
	if h.opts.ProviderAdminURL == "" {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "failure scenarios are off: DEMO_PROVIDER_ADMIN_URL is not set, or the workers deliver to another provider")
		return
	}
	var body io.Reader
	if r.Method == http.MethodPut {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxFaultsBody))
		if err != nil {
			if errors.As(err, new(*http.MaxBytesError)) {
				httpx.WriteError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body is too large")
				return
			}
			httpx.WriteError(w, http.StatusBadRequest, "invalid_request", "cannot read the request body")
			return
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method,
		strings.TrimRight(h.opts.ProviderAdminURL, "/")+"/admin/faults", body)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "internal", "cannot build the provider request")
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.opts.Client.Do(req) //nolint:gosec // G704: the URL is DEMO_PROVIDER_ADMIN_URL from the config, not from the request
	if err != nil {
		h.logger.WarnContext(r.Context(), "mock provider unreachable", "err", err)
		httpx.WriteError(w, http.StatusGatewayTimeout, "provider_unreachable", "the mock provider did not answer")
		return
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxFaultsBody+1))
	if err != nil || len(out) > maxFaultsBody {
		httpx.WriteError(w, http.StatusInternalServerError, "provider_error", "the mock provider's answer was cut off or too large")
		return
	}
	switch resp.StatusCode {
	case http.StatusOK:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(out)
	case http.StatusBadRequest:
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(out, &e) != nil || e.Error == "" {
			e.Error = "invalid rates"
		}
		httpx.WriteError(w, http.StatusBadRequest, "invalid_faults", "the mock provider rejected the rates: "+e.Error)
	default:
		h.logger.WarnContext(r.Context(), "mock provider answered unexpectedly", "status", resp.StatusCode)
		httpx.WriteError(w, http.StatusInternalServerError, "provider_error",
			"the mock provider answered "+http.StatusText(resp.StatusCode)+"; is DEMO_PROVIDER_ADMIN_URL the mock provider?")
	}
}

package playground

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/alpermelkeli/notification-system/internal/httpx"
)

// maxFaultsBody bounds the body forwarded to the mock provider.
const maxFaultsBody = 4 << 10

// handleFaults forwards GET and PUT to the mock provider's /admin/faults, which validates the
// rates. The browser cannot reach the mock provider itself: compose publishes only the load
// balancer.
func (h *Handler) handleFaults(w http.ResponseWriter, r *http.Request) {
	if h.opts.ProviderAdminURL == "" {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "failure scenarios are off: DEMO_PROVIDER_ADMIN_URL is not set")
		return
	}
	var body io.Reader
	if r.Method == http.MethodPut {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxFaultsBody))
		if err != nil {
			httpx.WriteError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body is too large")
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
		h.logger.Warn("mock provider unreachable", "err", err)
		httpx.WriteError(w, http.StatusBadGateway, "provider_unreachable", "the mock provider did not answer")
		return
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxFaultsBody))
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, "provider_unreachable", "the mock provider's answer was cut off")
		return
	}
	if resp.StatusCode != http.StatusOK {
		// The mock answers {"error": "..."}; pass its message on in the API's envelope.
		msg := strings.TrimSpace(string(out))
		httpx.WriteError(w, http.StatusBadRequest, "invalid_faults", "the mock provider rejected the rates: "+msg)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(out)
}

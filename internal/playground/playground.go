// Package playground serves /playground, a page that draws the delivery pipeline and moves each
// notification through it as its status events arrive over /v1/ws, with scenarios that run
// against the live system (ADR-0011). It is a local demo, off unless DEMO_ENABLED is set.
//
// Besides the static page it has two JSON endpoints: /playground/api/stats summarizes the
// fleet's Prometheus metrics (the browser cannot reach the dispatchers' and workers' /metrics),
// and /playground/api/faults reads and changes the mock provider's fault rates.
package playground

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

//go:embed static
var static embed.FS

// Paths served by the playground.
const (
	PathPage   = "/playground"
	pathAssets = "/playground/assets/"
	pathStats  = "/playground/api/stats"
	pathFaults = "/playground/api/faults"
)

// csp allows only the page's own files: no inline script or style, no third-party origin.
// connect-src 'self' covers the API, the JSON endpoints and the same-origin WebSocket.
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; " +
	"connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// Options configures the playground.
type Options struct {
	// PrometheusURL is the Prometheus server for the live counters; empty hides them.
	PrometheusURL string
	// ProviderAdminURL is the mock provider's base URL; empty disables the failure scenarios.
	ProviderAdminURL string
	// Client makes the upstream requests. Default: a client with a 3s timeout.
	Client *http.Client
}

// Handler serves the playground.
type Handler struct {
	logger *slog.Logger
	opts   Options
	assets http.Handler
	stats  *statsSource
}

// New returns a Handler.
func New(logger *slog.Logger, opts Options) *Handler {
	if opts.Client == nil {
		opts.Client = &http.Client{Timeout: 3 * time.Second}
	}
	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err) // the embedded tree is fixed at build time
	}
	return &Handler{
		logger: logger,
		opts:   opts,
		assets: http.StripPrefix(pathAssets, http.FileServerFS(sub)),
		stats:  newStatsSource(opts.Client, opts.PrometheusURL, time.Now),
	}
}

// Routes mounts the playground on r.
func (h *Handler) Routes(r chi.Router) {
	r.Get(PathPage, h.page)
	r.Get(pathAssets+"*", h.asset)
	r.Get(pathStats, h.handleStats)
	r.Get(pathFaults, h.handleFaults)
	r.Put(pathFaults, h.handleFaults)
}

func (h *Handler) page(w http.ResponseWriter, _ *http.Request) {
	body, err := static.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "page missing", http.StatusInternalServerError)
		return
	}
	securityHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(body)
}

func (h *Handler) asset(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == pathAssets || r.URL.Path[len(r.URL.Path)-1] == '/' {
		http.NotFound(w, r) // no directory listings
		return
	}
	securityHeaders(w)
	// The files change only with a new build; no-cache still lets the browser revalidate
	// cheaply (FileServer answers If-Modified-Since), so a redeploy is never hidden.
	w.Header().Set("Cache-Control", "no-cache")
	h.assets.ServeHTTP(w, r)
}

func securityHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Security-Policy", csp)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
}

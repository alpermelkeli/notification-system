package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/alpermelkeli/notification-system/internal/config"
)

// TestMountPlayground checks the DEMO_ENABLED gate, and that the failure scenarios are off when
// the workers deliver to a provider other than the mock whose faults they would change.
func TestMountPlayground(t *testing.T) {
	code := func(r http.Handler, path string) int {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	mock := "http://mockprovider:8081"
	tests := []struct {
		name        string
		cfg         config.Config
		page        int
		faultsRoute int // 404 when the route is absent or the faults are turned off
	}{
		{"off by default", config.Config{}, http.StatusNotFound, http.StatusNotFound},
		{"off with URLs set", config.Config{Demo: config.Demo{ProviderAdminURL: mock}, Worker: config.Worker{ProviderURL: mock + "/"}},
			http.StatusNotFound, http.StatusNotFound},
		{"on, delivering to the mock", config.Config{Demo: config.Demo{Enabled: true, ProviderAdminURL: "http://127.0.0.1:1"}, Worker: config.Worker{ProviderURL: "http://127.0.0.1:1/"}},
			http.StatusOK, http.StatusGatewayTimeout},
		{"on, delivering to webhook.site", config.Config{Demo: config.Demo{Enabled: true, ProviderAdminURL: mock}, Worker: config.Worker{ProviderURL: "https://webhook.site/x"}},
			http.StatusOK, http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chi.NewRouter()
			mountPlayground(r, tt.cfg, discard())
			if got := code(r, "/playground"); got != tt.page {
				t.Errorf("/playground: %d, want %d", got, tt.page)
			}
			if got := code(r, "/playground/assets/app.js"); tt.page == http.StatusNotFound && got != http.StatusNotFound {
				t.Errorf("assets served while the playground is off: %d", got)
			}
			if got := code(r, "/playground/api/faults"); got != tt.faultsRoute {
				t.Errorf("/playground/api/faults: %d, want %d", got, tt.faultsRoute)
			}
		})
	}
}

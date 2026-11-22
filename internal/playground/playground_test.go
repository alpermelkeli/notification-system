package playground

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func newRouter(opts Options) chi.Router {
	r := chi.NewRouter()
	New(slog.New(slog.DiscardHandler), opts).Routes(r)
	return r
}

func get(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func TestPageAndAssets(t *testing.T) {
	r := newRouter(Options{})

	page := get(t, r, http.MethodGet, PathPage, "")
	if page.Code != http.StatusOK || !strings.HasPrefix(page.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("page: %d %s", page.Code, page.Header().Get("Content-Type"))
	}
	csp := page.Header().Get("Content-Security-Policy")
	for _, want := range []string{"default-src 'none'", "script-src 'self'", "connect-src 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, want) {
			t.Errorf("CSP %q lacks %q", csp, want)
		}
	}
	if strings.Contains(csp, "unsafe-inline") || strings.Contains(csp, "http") {
		t.Errorf("CSP allows inline code or another origin: %q", csp)
	}
	html := page.Body.String()
	if strings.Contains(html, "<script>") || strings.Contains(html, "style=") {
		t.Error("the page has inline script or style, which the CSP blocks")
	}

	// Every file the page references exists and is served with a sensible type.
	refs := regexp.MustCompile(`(?:href|src)="(/playground/assets/[^"]+)"`).FindAllStringSubmatch(html, -1)
	if len(refs) < 3 {
		t.Fatalf("expected the stylesheet, script and font to be referenced, got %v", refs)
	}
	css := get(t, r, http.MethodGet, "/playground/assets/style.css", "").Body.String()
	for _, m := range regexp.MustCompile(`url\("([^"]+)"\)`).FindAllStringSubmatch(css, -1) {
		refs = append(refs, []string{"", "/playground/assets/" + m[1]})
	}
	types := map[string]string{".css": "text/css", ".js": "text/javascript", ".woff2": "font/woff2"}
	for _, ref := range refs {
		rec := get(t, r, http.MethodGet, ref[1], "")
		if rec.Code != http.StatusOK {
			t.Errorf("%s: %d", ref[1], rec.Code)
			continue
		}
		for ext, typ := range types {
			if strings.HasSuffix(ref[1], ext) && !strings.HasPrefix(rec.Header().Get("Content-Type"), typ) {
				t.Errorf("%s: Content-Type %q, want %s", ref[1], rec.Header().Get("Content-Type"), typ)
			}
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: missing nosniff", ref[1])
		}
	}

	for _, path := range []string{"/playground/assets/", "/playground/assets/fonts/", "/playground/assets/missing.js"} {
		if rec := get(t, r, http.MethodGet, path, ""); rec.Code != http.StatusNotFound {
			t.Errorf("%s: %d, want 404", path, rec.Code)
		}
	}
}

func TestFontLicensesAreShipped(t *testing.T) {
	for _, name := range []string{"static/fonts/Inter-OFL.txt", "static/fonts/JetBrainsMono-OFL.txt"} {
		b, err := static.ReadFile(name)
		if err != nil || !strings.Contains(string(b), "SIL Open Font License") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// fakePrometheus answers instant queries with one sample per known query.
func fakePrometheus(t *testing.T, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	answers := map[string]string{
		statsQueries["instances"]:     `[{"metric":{"job":"api"},"value":[1,"1"]},{"metric":{"job":"worker"},"value":[1,"3"]}]`,
		statsQueries["notifications"]: `[{"metric":{"status":"pending"},"value":[1,"12"]}]`,
		statsQueries["lag"]:           `[{"metric":{"topic":"notifications.sms.low"},"value":[1,"40"]}]`,
		statsQueries["created"]:       `[{"metric":{},"value":[1,"250.5"]}]`,
		statsQueries["delivered"]:     `[{"metric":{"channel":"sms"},"value":[1,"99.8"]}]`,
		statsQueries["calls"]:         `[{"metric":{"channel":"sms","class":"delivered"},"value":[1,"1000"]},{"metric":{"channel":"sms","class":"retryable"},"value":[1,"7"]}]`,
		statsQueries["breakers"]:      `[{"metric":{"channel":"sms"},"value":[1,"2"]},{"metric":{"channel":"email"},"value":[1,"0"]}]`,
		statsQueries["deadLetters"]:   `[]`,
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		result, ok := answers[r.URL.Query().Get("query")]
		if !ok {
			t.Errorf("unexpected query %q", r.URL.Query().Get("query"))
			result = "[]"
		}
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":`+result+`}}`)
	}))
}

func TestStats(t *testing.T) {
	var calls atomic.Int32
	prom := fakePrometheus(t, &calls)
	defer prom.Close()
	r := newRouter(Options{PrometheusURL: prom.URL + "/"})

	rec := get(t, r, http.MethodGet, PathStats, "")
	var st Stats
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &st) != nil {
		t.Fatalf("stats: %d %s", rec.Code, rec.Body.String())
	}
	if !st.Available || st.Error != "" {
		t.Fatalf("stats unavailable: %+v", st)
	}
	if st.Instances["worker"] != 3 || st.Notifications["pending"] != 12 || st.KafkaLag["notifications.sms.low"] != 40 ||
		st.CreatedPerSecond != 250.5 || st.DeliveredPerSecond["sms"] != 99.8 ||
		st.ProviderCalls["sms"]["delivered"] != 1000 || st.ProviderCalls["sms"]["retryable"] != 7 ||
		st.Breakers["sms"] != "open" || st.Breakers["email"] != "closed" || st.DeadLetters != 0 {
		t.Errorf("stats = %+v", st)
	}

	// A second request within the cache TTL reuses the answers.
	before := calls.Load()
	get(t, r, http.MethodGet, PathStats, "")
	if calls.Load() != before {
		t.Errorf("stats were queried again within %v", statsCacheTTL)
	}
}

func TestStatsCacheExpires(t *testing.T) {
	var calls atomic.Int32
	prom := fakePrometheus(t, &calls)
	defer prom.Close()
	now := time.Unix(1000, 0)
	s := newStatsSource(prom.Client(), prom.URL, func() time.Time { return now })
	s.get(t.Context())
	first := calls.Load()
	now = now.Add(statsCacheTTL)
	s.get(t.Context())
	if calls.Load() != 2*first {
		t.Errorf("queries after the TTL: %d, want %d", calls.Load(), 2*first)
	}
}

func TestStatsWithoutPrometheus(t *testing.T) {
	rec := get(t, newRouter(Options{}), http.MethodGet, PathStats, "")
	var st Stats
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &st) != nil || st.Available ||
		!strings.Contains(st.Error, "DEMO_PROMETHEUS_URL") {
		t.Errorf("unconfigured stats: %d %s", rec.Code, rec.Body.String())
	}

	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	rec = get(t, newRouter(Options{PrometheusURL: down.URL}), http.MethodGet, PathStats, "")
	st = Stats{}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &st) != nil || st.Available ||
		!strings.Contains(st.Error, "503") {
		t.Errorf("stats with Prometheus down: %d %s", rec.Code, rec.Body.String())
	}
}

func TestFaultsProxy(t *testing.T) {
	var gotBody, gotMethod string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/faults" {
			http.NotFound(w, r)
			return
		}
		gotMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if strings.Contains(gotBody, "teapot") {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		if strings.Contains(gotBody, "big") {
			_, _ = io.WriteString(w, strings.Repeat("x", maxFaultsBody+1))
			return
		}
		if strings.Contains(gotBody, "2") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":"failureRate must be between 0 and 1"}`)
			return
		}
		_, _ = io.WriteString(w, `{"failureRate":1,"rateLimitRate":0,"permanentRate":0}`)
	}))
	defer upstream.Close()
	r := newRouter(Options{ProviderAdminURL: upstream.URL + "/"})

	if rec := get(t, r, http.MethodGet, PathFaults, ""); rec.Code != http.StatusOK || gotMethod != http.MethodGet ||
		!strings.Contains(rec.Body.String(), `"failureRate":1`) {
		t.Errorf("GET: %d %s", rec.Code, rec.Body.String())
	}
	body := `{"failureRate":1,"rateLimitRate":0,"permanentRate":0}`
	if rec := get(t, r, http.MethodPut, PathFaults, body); rec.Code != http.StatusOK || gotMethod != http.MethodPut || gotBody != body {
		t.Errorf("PUT: %d %s (forwarded %s %q)", rec.Code, rec.Body.String(), gotMethod, gotBody)
	}
	rec := get(t, r, http.MethodPut, PathFaults, `{"failureRate":2,"rateLimitRate":0,"permanentRate":0}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid_faults") ||
		!strings.Contains(rec.Body.String(), "between 0 and 1") || strings.Contains(rec.Body.String(), `{\"error`) {
		t.Errorf("rejected PUT: %d %s", rec.Code, rec.Body.String())
	}
	// Upstream answers other than 200 and 400 are not the client's fault, and never 502 or 503,
	// which nginx would count against the API replica.
	for _, body := range []string{"teapot", "big"} {
		if rec := get(t, r, http.MethodPut, PathFaults, body); rec.Code != http.StatusInternalServerError ||
			!strings.Contains(rec.Body.String(), "provider_error") {
			t.Errorf("upstream %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	if rec := get(t, r, http.MethodPut, PathFaults, strings.Repeat("x", maxFaultsBody+1)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized PUT: %d", rec.Code)
	}

	if rec := get(t, newRouter(Options{}), http.MethodGet, PathFaults, ""); rec.Code != http.StatusNotFound {
		t.Errorf("unconfigured: %d, want 404", rec.Code)
	}
	upstream.Close()
	if rec := get(t, r, http.MethodGet, PathFaults, ""); rec.Code != http.StatusGatewayTimeout {
		t.Errorf("mock provider down: %d, want 504", rec.Code)
	}
}

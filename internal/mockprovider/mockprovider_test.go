package mockprovider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const validBody = `{"to":"+905551234567","channel":"sms","content":"hi"}`

func newServer(cfg Config, roll float64) *Server {
	s := New(cfg, prometheus.NewRegistry())
	s.rand = func() float64 { return roll }
	return s
}

func post(t *testing.T, h http.Handler, body, idemKey string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/some-uuid", strings.NewReader(body))
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSend(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		roll       float64
		body       string
		wantCode   int
		retryAfter string
	}{
		{"accepted", Config{}, 0.5, validBody, http.StatusAccepted, ""},
		{"invalid json", Config{}, 0.5, `{`, http.StatusBadRequest, ""},
		{"missing to", Config{}, 0.5, `{"channel":"sms","content":"x"}`, http.StatusBadRequest, ""},
		{"unknown channel", Config{}, 0.5, `{"to":"a","channel":"fax","content":"x"}`, http.StatusBadRequest, ""},
		{"rate limited", Config{RateLimitRate: 0.3, RetryAfter: 2 * time.Second}, 0.2, validBody, http.StatusTooManyRequests, "2"},
		{"failure", Config{RateLimitRate: 0.3, FailureRate: 0.3}, 0.5, validBody, http.StatusInternalServerError, ""},
		{"roll above both rates", Config{RateLimitRate: 0.3, FailureRate: 0.3}, 0.7, validBody, http.StatusAccepted, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := post(t, newServer(tt.cfg, tt.roll).Handler(), tt.body, "")
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (%s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if got := rec.Header().Get("Retry-After"); got != tt.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tt.retryAfter)
			}
			if tt.wantCode == http.StatusAccepted {
				var resp Response
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				if resp.MessageID == "" || resp.Status != "accepted" {
					t.Errorf("response = %+v", resp)
				}
				if _, err := time.Parse(time.RFC3339Nano, resp.Timestamp); err != nil {
					t.Errorf("timestamp %q: %v", resp.Timestamp, err)
				}
			}
		})
	}
}

func TestIdempotencyKeyReturnsSameMessageID(t *testing.T) {
	h := newServer(Config{}, 0.5).Handler()
	id := func(key string) string {
		var resp Response
		_ = json.Unmarshal(post(t, h, validBody, key).Body.Bytes(), &resp)
		return resp.MessageID
	}
	a1, a2, b := id("key-a"), id("key-a"), id("key-b")
	if a1 != a2 {
		t.Errorf("same key gave %s and %s", a1, a2)
	}
	if a1 == b {
		t.Error("different keys gave the same messageId")
	}
	if n1, n2 := id(""), id(""); n1 == n2 {
		t.Error("requests without a key must get distinct ids")
	}
}

func TestStats(t *testing.T) {
	s := newServer(Config{}, 0.5)
	now := time.Unix(1_000, 0)
	s.now = func() time.Time { return now }
	h := s.Handler()

	for range 3 {
		post(t, h, validBody, "")
	}
	now = now.Add(time.Second)
	post(t, h, validBody, "")
	s.cfg.FailureRate = 1
	post(t, h, validBody, "")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var st Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.Accepted["sms"] != 4 || st.Rejected["sms"] != 1 {
		t.Errorf("totals = %+v", st)
	}
	if st.RateLastSecond["sms"] != 3 {
		t.Errorf("rate_last_second = %d, want 3", st.RateLastSecond["sms"])
	}
}

func TestSecondCounter(t *testing.T) {
	var c secondCounter
	c.add(10)
	c.add(10)
	c.add(11)
	if c.count(10) != 2 || c.count(11) != 1 || c.count(9) != 0 {
		t.Errorf("counts: 10=%d 11=%d", c.count(10), c.count(11))
	}
	c.add(15) // gap: older seconds are forgotten
	if c.count(14) != 0 || c.count(15) != 1 {
		t.Errorf("after gap: 14=%d 15=%d", c.count(14), c.count(15))
	}
}

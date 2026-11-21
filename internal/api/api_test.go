package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/httpx"
	"github.com/alpermelkeli/notification-system/internal/storage"
)

var testNow = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

type testServer struct {
	store *fakeStore
	h     http.Handler
}

func newServer(t *testing.T) *testServer {
	t.Helper()
	store := newFakeStore()
	r := chi.NewRouter()
	r.Use(httpx.CorrelationID)
	New(store, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		MaxScheduleAhead: 24 * time.Hour, RequestTimeout: 200 * time.Millisecond,
		Now: func() time.Time { return testNow },
	}).Routes(r)
	return &testServer{store: store, h: r}
}

type response struct {
	*httptest.ResponseRecorder
	t *testing.T
}

func (s *testServer) do(t *testing.T, method, path, body string, headers ...string) response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	s.h.ServeHTTP(rec, req)
	return response{rec, t}
}

func (r response) decode(v any) {
	r.t.Helper()
	if err := json.Unmarshal(r.Body.Bytes(), v); err != nil {
		r.t.Fatalf("decode %s: %v", r.Body.String(), err)
	}
}

func (r response) wantStatus(code int) response {
	r.t.Helper()
	if r.Code != code {
		r.t.Fatalf("status = %d, want %d; body: %s", r.Code, code, r.Body.String())
	}
	return r
}

func (r response) errorBody() httpx.ErrorDetail {
	r.t.Helper()
	var b httpx.ErrorBody
	r.decode(&b)
	return b.Error
}

const otp = `{"recipient":"+905551234567","channel":"sms","content":"Your code is 482913","priority":"high"}`

func TestCreateNotification(t *testing.T) {
	s := newServer(t)
	res := s.do(t, http.MethodPost, "/v1/notifications", otp, "X-Correlation-ID", "req-1").wantStatus(http.StatusAccepted)

	var n notificationJSON
	res.decode(&n)
	if n.Status != "pending" || n.Priority != "high" || n.Channel != "sms" || n.CorrelationID != "req-1" {
		t.Errorf("unexpected body: %+v", n)
	}
	if got := res.Header().Get("Location"); got != "/v1/notifications/"+n.ID.String() {
		t.Errorf("Location = %q", got)
	}
	if res.Header().Get(headerIdempotentReplay) != "" {
		t.Error("a fresh create must not be marked as replayed")
	}
	if s.store.lastCorrelation != "req-1" {
		t.Errorf("correlation id passed to the store = %q", s.store.lastCorrelation)
	}
}

func TestCreateDefaultsPriorityToNormal(t *testing.T) {
	s := newServer(t)
	s.do(t, http.MethodPost, "/v1/notifications",
		`{"recipient":"user@example.com","channel":"email","content":"hi"}`).wantStatus(http.StatusAccepted)
	if s.store.lastCreate.Priority != domain.PriorityNormal {
		t.Errorf("priority = %q", s.store.lastCreate.Priority)
	}
}

func TestCreateIdempotency(t *testing.T) {
	s := newServer(t)
	first := s.do(t, http.MethodPost, "/v1/notifications", otp, "Idempotency-Key", "k1").wantStatus(http.StatusAccepted)
	// Same request with the fields in another order: same hash.
	same := `{"priority":"high","content":"Your code is 482913","channel":"sms","recipient":"+905551234567"}`
	again := s.do(t, http.MethodPost, "/v1/notifications", same, "Idempotency-Key", "k1").wantStatus(http.StatusAccepted)

	var a, b notificationJSON
	first.decode(&a)
	again.decode(&b)
	if a.ID != b.ID || again.Header().Get(headerIdempotentReplay) != "true" {
		t.Fatalf("replay returned %s (replayed=%q), want %s", b.ID, again.Header().Get(headerIdempotentReplay), a.ID)
	}

	other := strings.Replace(otp, "482913", "000000", 1)
	e := s.do(t, http.MethodPost, "/v1/notifications", other, "Idempotency-Key", "k1").
		wantStatus(http.StatusConflict).errorBody()
	if e.Code != "idempotency_key_reused" {
		t.Errorf("code = %q", e.Code)
	}
}

func TestCreateValidation(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantFields []string
	}{
		{"missing everything", `{}`, []string{"channel"}},
		{"bad phone", `{"recipient":"0555","channel":"sms","content":"x"}`, []string{"recipient"}},
		{"bad channel and priority", `{"recipient":"x","channel":"fax","content":"x","priority":"urgent"}`, []string{"channel", "priority"}},
		{"empty content", `{"recipient":"user@example.com","channel":"email","content":" "}`, []string{"content"}},
		{"sms too long", fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":%q}`, strings.Repeat("ş", 700)), []string{"content"}},
		{"scheduled too far", `{"recipient":"+905551234567","channel":"sms","content":"x","scheduledAt":"2026-12-30T00:00:00Z"}`, []string{"scheduledAt"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newServer(t)
			e := s.do(t, http.MethodPost, "/v1/notifications", tt.body).wantStatus(http.StatusUnprocessableEntity).errorBody()
			if e.Code != "validation_failed" {
				t.Fatalf("code = %q", e.Code)
			}
			got := fields(t, e.Details)
			if strings.Join(got, ",") != strings.Join(tt.wantFields, ",") {
				t.Errorf("violated fields = %v, want %v (details %v)", got, tt.wantFields, e.Details)
			}
		})
	}
}

// TestReplayAfterScheduledTimePassed: a past scheduledAt means "as soon as possible", so a
// retry that arrives after the original schedule still replays instead of failing with 422.
func TestReplayAfterScheduledTimePassed(t *testing.T) {
	s := newServer(t)
	body := `{"recipient":"+905551234567","channel":"sms","content":"x","scheduledAt":"2026-11-21T12:00:30Z"}`
	var first, again notificationJSON
	s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "sched").wantStatus(http.StatusAccepted).decode(&first)
	testNow = testNow.Add(10 * time.Minute)
	defer func() { testNow = testNow.Add(-10 * time.Minute) }()
	res := s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "sched").wantStatus(http.StatusAccepted)
	res.decode(&again)
	if again.ID != first.ID || res.Header().Get(headerIdempotentReplay) != "true" {
		t.Errorf("late retry was not replayed: %s vs %s", again.ID, first.ID)
	}
}

func TestCreateAcceptsScheduledNotification(t *testing.T) {
	s := newServer(t)
	s.do(t, http.MethodPost, "/v1/notifications",
		`{"recipient":"+905551234567","channel":"sms","content":"x","scheduledAt":"2026-11-21T18:00:00.1234567+03:00"}`).
		wantStatus(http.StatusAccepted)
	got := s.store.lastCreate.ScheduledAt
	want := time.Date(2026, 11, 21, 15, 0, 0, 123456000, time.UTC)
	if got == nil || !got.Equal(want) || got.Location() != time.UTC {
		t.Errorf("scheduledAt = %v, want %v normalised to UTC microseconds", got, want)
	}
}

func TestMalformedRequests(t *testing.T) {
	s := newServer(t)
	tests := []struct {
		name     string
		body     string
		headers  []string
		wantCode int
		wantErr  string
	}{
		{"not json", `{"recipient":`, nil, http.StatusBadRequest, "bad_request"},
		{"unknown field", `{"recipient":"+905551234567","channel":"sms","content":"x","prio":"high"}`, nil, http.StatusBadRequest, "bad_request"},
		{"trailing data", otp + otp, nil, http.StatusBadRequest, "bad_request"},
		{"wrong type", `{"recipient":5}`, nil, http.StatusBadRequest, "bad_request"},
		{"null body", `null`, nil, http.StatusUnprocessableEntity, "validation_failed"},
		{"too large", fmt.Sprintf(`{"content":%q}`, strings.Repeat("x", maxSingleBody)), nil, http.StatusRequestEntityTooLarge, "payload_too_large"},
		{"bad idempotency key", otp, []string{"Idempotency-Key", strings.Repeat("k", 256)}, http.StatusBadRequest, "bad_request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := s.do(t, http.MethodPost, "/v1/notifications", tt.body, tt.headers...).wantStatus(tt.wantCode).errorBody()
			if e.Code != tt.wantErr {
				t.Errorf("code = %q, want %q (%s)", e.Code, tt.wantErr, e.Message)
			}
		})
	}

	t.Run("wrong content type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(otp))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		s.h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnsupportedMediaType {
			t.Errorf("status = %d", rec.Code)
		}
	})
}

func TestCreateBatch(t *testing.T) {
	s := newServer(t)
	body := `{"notifications":[
		{"recipient":"+905551234567","channel":"sms","content":"sale","priority":"low"},
		{"recipient":"user@example.com","channel":"email","content":"sale","idempotencyKey":"item-1"}]}`
	res := s.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "b1").wantStatus(http.StatusAccepted)

	var out batchCreatedJSON
	res.decode(&out)
	if out.Batch.Total != 2 || len(out.Items) != 2 || out.Items[1].Index != 1 || out.Batch.StatusCounts["pending"] != 2 {
		t.Fatalf("unexpected body: %+v", out)
	}
	if res.Header().Get("Location") != "/v1/batches/"+out.Batch.ID.String() {
		t.Errorf("Location = %q", res.Header().Get("Location"))
	}
	items := s.store.lastBatch.Items
	if items[0].IdempotencyKey != nil || items[1].IdempotencyKey == nil || items[1].IdempotencyKey.Key != "item-1" {
		t.Errorf("item keys not passed through: %+v", items)
	}

	replay := s.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "b1").wantStatus(http.StatusAccepted)
	if replay.Header().Get(headerIdempotentReplay) != "true" {
		t.Error("batch replay must be marked")
	}
	// The item key is part of the batch hash: changing it is a different request.
	changed := strings.Replace(body, "item-1", "item-2", 1)
	s.do(t, http.MethodPost, "/v1/notifications/batch", changed, "Idempotency-Key", "b1").wantStatus(http.StatusConflict)
}

func TestDecodeErrorsDoNotLeakGoTypes(t *testing.T) {
	s := newServer(t)
	e := s.do(t, http.MethodPost, "/v1/notifications", `{"recipient":5}`).wantStatus(http.StatusBadRequest).errorBody()
	if strings.Contains(e.Message, "createRequest") || strings.Contains(e.Message, "Go ") || !strings.Contains(e.Message, `"recipient"`) {
		t.Errorf("message = %q", e.Message)
	}
	e = s.do(t, http.MethodPost, "/v1/notifications", `{"prio":"high"}`).wantStatus(http.StatusBadRequest).errorBody()
	if e.Message != `unknown field "prio"` {
		t.Errorf("message = %q", e.Message)
	}
}

func TestRequestDeadline(t *testing.T) {
	s := newServer(t)
	s.store.block = true
	e := s.do(t, http.MethodPost, "/v1/notifications", otp).wantStatus(http.StatusServiceUnavailable).errorBody()
	if e.Code != "timeout" {
		t.Errorf("code = %q", e.Code)
	}
}

func TestBatchItemKeyConflictPointsAtTheItem(t *testing.T) {
	s := newServer(t)
	s.store.err = &storage.ItemError{Index: 3, Err: domain.ErrIdempotencyKeyReused}
	e := s.do(t, http.MethodPost, "/v1/notifications/batch",
		`{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"x"}]}`).
		wantStatus(http.StatusConflict).errorBody()
	if e.Code != "idempotency_key_reused" || strings.Join(fields(t, e.Details), ",") != "notifications[3].idempotencyKey" {
		t.Errorf("error = %+v", e)
	}
}

func TestCreateBatchValidation(t *testing.T) {
	s := newServer(t)
	body := `{"notifications":[
		{"recipient":"+905551234567","channel":"sms","content":"ok"},
		{"recipient":"bad","channel":"sms","content":"x","idempotencyKey":"a"},
		{"recipient":"user@example.com","channel":"email","content":"x","idempotencyKey":"a"},
		{"recipient":"user@example.com","channel":"email","content":"x","idempotencyKey":"hdr"},
		{"recipient":"user@example.com","channel":"email","content":"x","idempotencyKey":""}]}`
	e := s.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "hdr").
		wantStatus(http.StatusUnprocessableEntity).errorBody()
	want := "notifications[1].recipient,notifications[2].idempotencyKey,notifications[3].idempotencyKey,notifications[4].idempotencyKey"
	if got := strings.Join(fields(t, e.Details), ","); got != want {
		t.Errorf("fields = %s, want %s", got, want)
	}
	if s.store.lastBatch.Items != nil {
		t.Error("nothing may be stored when an item is invalid")
	}

	for name, n := range map[string]int{"empty": 0, "too large": storage.MaxBatchSize + 1} {
		t.Run(name, func(t *testing.T) {
			items := strings.TrimSuffix(strings.Repeat(`{"recipient":"+905551234567","channel":"sms","content":"x"},`, n), ",")
			e := s.do(t, http.MethodPost, "/v1/notifications/batch", `{"notifications":[`+items+`]}`).
				wantStatus(http.StatusUnprocessableEntity).errorBody()
			if f := fields(t, e.Details); len(f) != 1 || f[0] != "notifications" {
				t.Errorf("fields = %v", f)
			}
		})
	}
}

func TestGetCancelRetryHistory(t *testing.T) {
	s := newServer(t)
	var n notificationJSON
	s.do(t, http.MethodPost, "/v1/notifications", otp).wantStatus(http.StatusAccepted).decode(&n)
	path := "/v1/notifications/" + n.ID.String()

	var got notificationJSON
	s.do(t, http.MethodGet, path, "").wantStatus(http.StatusOK).decode(&got)
	if got.ID != n.ID {
		t.Errorf("get returned %s", got.ID)
	}
	var h historyJSON
	s.do(t, http.MethodGet, path+"/history", "").wantStatus(http.StatusOK).decode(&h)
	if h.ID != n.ID || len(h.Changes) != 1 || h.Changes[0].To != "pending" {
		t.Errorf("history = %+v", h)
	}

	// Only failed notifications can be replayed.
	e := s.do(t, http.MethodPost, path+"/retry", "").wantStatus(http.StatusConflict).errorBody()
	if e.Code != "invalid_state" || !strings.Contains(e.Message, "failed") {
		t.Errorf("retry pending: %+v", e)
	}

	s.do(t, http.MethodPost, path+"/cancel", "").wantStatus(http.StatusOK).decode(&got)
	if got.Status != "cancelled" {
		t.Errorf("status after cancel = %s", got.Status)
	}
	e = s.do(t, http.MethodPost, path+"/cancel", "").wantStatus(http.StatusConflict).errorBody()
	if e.Code != "invalid_state" || fmt.Sprint(e.Details) != "[map[currentStatus:cancelled]]" {
		t.Errorf("second cancel: %+v", e)
	}

	s.store.setStatus(n.ID, domain.StatusFailed)
	s.do(t, http.MethodPost, path+"/retry", "").wantStatus(http.StatusAccepted).decode(&got)
	if got.Status != "pending" {
		t.Errorf("status after retry = %s", got.Status)
	}
}

func TestNotFound(t *testing.T) {
	s := newServer(t)
	for _, path := range []string{
		"/v1/notifications/" + domain.NewIDAt(time.Now()).String(),
		"/v1/notifications/not-a-uuid",
		"/v1/notifications/not-a-uuid/history",
		"/v1/batches/" + uuid.NewString(),
	} {
		if e := s.do(t, http.MethodGet, path, "").wantStatus(http.StatusNotFound).errorBody(); e.Code != "not_found" {
			t.Errorf("%s: code %q", path, e.Code)
		}
	}
	s.do(t, http.MethodPost, "/v1/notifications/"+uuid.NewString()+"/cancel", "").wantStatus(http.StatusNotFound)
}

func TestGetBatch(t *testing.T) {
	s := newServer(t)
	var out batchCreatedJSON
	s.do(t, http.MethodPost, "/v1/notifications/batch",
		`{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"x"}]}`).
		wantStatus(http.StatusAccepted).decode(&out)
	var b batchJSON
	s.do(t, http.MethodGet, "/v1/batches/"+out.Batch.ID.String(), "").wantStatus(http.StatusOK).decode(&b)
	if b.ID != out.Batch.ID || b.Total != 1 || b.StatusCounts["pending"] != 1 {
		t.Errorf("batch = %+v", b)
	}
}

func TestListNotifications(t *testing.T) {
	s := newServer(t)
	last := domain.NewIDAt(time.Now())
	s.store.listPage = storage.ListPage{Items: []domain.Notification{{ID: last, Status: domain.StatusSent}}, Next: &last}

	batch := uuid.New()
	cursor := encodeCursor(domain.NewIDAt(time.Now()))
	var out listJSON
	s.do(t, http.MethodGet, "/v1/notifications?status=sent&channel=email&batchId="+batch.String()+
		"&createdFrom=2026-11-01T00:00:00Z&createdTo=2026-11-21T00:00:00%2B03:00&limit=20&cursor="+cursor, "").
		wantStatus(http.StatusOK).decode(&out)

	f := s.store.lastFilter
	if *f.Status != domain.StatusSent || *f.Channel != domain.ChannelEmail || *f.BatchID != batch || f.Limit != 20 ||
		!f.CreatedTo.Equal(time.Date(2026, 11, 20, 21, 0, 0, 0, time.UTC)) || f.After == nil {
		t.Errorf("filter = %+v", f)
	}
	if len(out.Items) != 1 || out.NextCursor != encodeCursor(last) {
		t.Errorf("list = %+v", out)
	}

	// The cursor round-trips.
	if id, ok := decodeCursor(out.NextCursor); !ok || id != last {
		t.Errorf("decodeCursor = %s, %v", id, ok)
	}
}

func TestListRejectsBadQueries(t *testing.T) {
	s := newServer(t)
	for _, q := range []string{
		"status=done", "channel=fax", "batchId=x", "createdFrom=yesterday", "limit=0", "limit=501", "limit=ten",
		"createdFrom=2026-11-21T00:00:00Z&createdTo=2026-11-21T00:00:00Z",
		"cursor=!!", "cursor=" + encodeCursor(uuid.New()), // a v4 id cannot be a cursor
	} {
		if e := s.do(t, http.MethodGet, "/v1/notifications?"+q, "").wantStatus(http.StatusBadRequest).errorBody(); e.Code != "bad_request" {
			t.Errorf("%s: code %q", q, e.Code)
		}
	}
}

func TestStoreErrorsAreNotLeaked(t *testing.T) {
	s := newServer(t)
	s.store.err = errors.New("pq: connection refused to 10.0.0.5")
	e := s.do(t, http.MethodPost, "/v1/notifications", otp).wantStatus(http.StatusInternalServerError).errorBody()
	if e.Code != "internal_error" || strings.Contains(e.Message, "10.0.0.5") {
		t.Errorf("internal error leaked details: %+v", e)
	}
}

func TestDocsAreServed(t *testing.T) {
	s := newServer(t)
	if res := s.do(t, http.MethodGet, "/openapi.yaml", "").wantStatus(http.StatusOK); !strings.HasPrefix(res.Body.String(), "openapi: 3.1.0") {
		t.Errorf("spec body starts with %.30q", res.Body.String())
	}
	res := s.do(t, http.MethodGet, "/docs", "").wantStatus(http.StatusOK)
	body := res.Body.String()
	if !strings.Contains(body, "swagger-ui") || strings.Count(body, `integrity="sha384-`) != 2 {
		t.Error("docs page must load Swagger UI with Subresource Integrity on both assets")
	}
	csp := res.Header().Get("Content-Security-Policy")
	sum := sha256.Sum256([]byte(swaggerInit))
	if !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, base64.StdEncoding.EncodeToString(sum[:])) {
		t.Errorf("CSP = %q", csp)
	}
}

// fields extracts the field names from validation details.
func fields(t *testing.T, details []any) []string {
	t.Helper()
	var out []string
	for _, d := range details {
		m, ok := d.(map[string]any)
		if !ok {
			t.Fatalf("detail %v is not an object", d)
		}
		out = append(out, fmt.Sprint(m["field"]))
	}
	return out
}

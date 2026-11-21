package api

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// createTemplate creates a template through the API and returns it.
func (s *testServer) createTemplate(t *testing.T, name, channel, body string) templateJSON {
	t.Helper()
	var tpl templateJSON
	s.do(t, http.MethodPost, "/v1/templates",
		fmt.Sprintf(`{"name":%q,"channel":%q,"body":%q}`, name, channel, body)).wantStatus(http.StatusCreated).decode(&tpl)
	return tpl
}

func TestTemplateCRUD(t *testing.T) {
	s := newServer(t)

	res := s.do(t, http.MethodPost, "/v1/templates",
		`{"name":"order_shipped","channel":"sms","body":"Order {{order_id}} shipped, {{ name }}"}`).wantStatus(http.StatusCreated)
	var tpl templateJSON
	res.decode(&tpl)
	if res.Header().Get("Location") != "/v1/templates/"+tpl.ID.String() {
		t.Errorf("Location = %q", res.Header().Get("Location"))
	}
	if tpl.Version != 1 || !slices.Equal(tpl.Variables, []string{"order_id", "name"}) {
		t.Errorf("unexpected template: %+v", tpl)
	}

	if e := s.do(t, http.MethodPost, "/v1/templates", `{"name":"order_shipped","channel":"sms","body":"x"}`).
		wantStatus(http.StatusConflict).errorBody(); e.Code != "already_exists" {
		t.Errorf("duplicate name: code %q", e.Code)
	}

	var got templateJSON
	s.do(t, http.MethodGet, "/v1/templates/"+tpl.ID.String(), "").wantStatus(http.StatusOK).decode(&got)
	if got.Name != "order_shipped" || got.Body != tpl.Body {
		t.Errorf("get: %+v", got)
	}

	var upd templateJSON
	s.do(t, http.MethodPut, "/v1/templates/"+tpl.ID.String(),
		`{"name":"order_shipped","channel":"sms","body":"Shipped: {{order_id}}"}`).wantStatus(http.StatusOK).decode(&upd)
	if upd.Version != 2 || !slices.Equal(upd.Variables, []string{"order_id"}) {
		t.Errorf("update: %+v", upd)
	}
	assertViolations(t, s.do(t, http.MethodPut, "/v1/templates/"+tpl.ID.String(),
		`{"name":"order_shipped","channel":"email","body":"x"}`), "channel")

	s.do(t, http.MethodDelete, "/v1/templates/"+tpl.ID.String(), "").wantStatus(http.StatusNoContent)
	s.do(t, http.MethodGet, "/v1/templates/"+tpl.ID.String(), "").wantStatus(http.StatusNotFound)
	s.do(t, http.MethodDelete, "/v1/templates/"+tpl.ID.String(), "").wantStatus(http.StatusNotFound)
	s.do(t, http.MethodPut, "/v1/templates/"+tpl.ID.String(),
		`{"name":"order_shipped","channel":"sms","body":"x"}`).wantStatus(http.StatusNotFound)
	s.do(t, http.MethodGet, "/v1/templates/not-a-uuid", "").wantStatus(http.StatusNotFound)
	s.do(t, http.MethodDelete, "/v1/templates/not-a-uuid", "").wantStatus(http.StatusNotFound)
}

func TestTemplateValidation(t *testing.T) {
	s := newServer(t)
	tests := []struct {
		name, body string
		fields     []string
	}{
		{"everything missing", `{}`, []string{"name", "channel"}},
		{"bad name", `{"name":"Order Shipped","channel":"sms","body":"x"}`, []string{"name"}},
		{"bad channel", `{"name":"a","channel":"fax","body":"x"}`, []string{"channel"}},
		{"malformed placeholder", `{"name":"a","channel":"sms","body":"Hi {{first name}}"}`, []string{"body"}},
		{"unclosed placeholder", `{"name":"a","channel":"sms","body":"Hi {{name"}`, []string{"body"}},
		{"empty body", `{"name":"a","channel":"push","body":" "}`, []string{"body"}},
		{"body over the channel limit", `{"name":"a","channel":"sms","body":"` + strings.Repeat("x", 1531) + `"}`, []string{"body"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertViolations(t, s.do(t, http.MethodPost, "/v1/templates", tt.body), tt.fields...)
		})
	}
	s.do(t, http.MethodPost, "/v1/templates", `{"name":"a","channel":"sms","body":"x","extra":1}`).wantStatus(http.StatusBadRequest)
	s.do(t, http.MethodGet, "/v1/templates?channel=fax", "").wantStatus(http.StatusBadRequest)
	s.do(t, http.MethodGet, "/v1/templates?limit=0", "").wantStatus(http.StatusBadRequest)
	s.do(t, http.MethodGet, "/v1/templates?cursor=!!", "").wantStatus(http.StatusBadRequest)
}

func TestListTemplates(t *testing.T) {
	s := newServer(t)
	s.createTemplate(t, "a", "sms", "x")
	s.createTemplate(t, "b", "email", "y")

	var all templateListJSON
	s.do(t, http.MethodGet, "/v1/templates", "").wantStatus(http.StatusOK).decode(&all)
	if len(all.Items) != 2 {
		t.Errorf("got %d templates, want 2", len(all.Items))
	}
	var email templateListJSON
	s.do(t, http.MethodGet, "/v1/templates?channel=email&limit=10", "").wantStatus(http.StatusOK).decode(&email)
	if len(email.Items) != 1 || email.Items[0].Name != "b" || email.Items[0].Variables == nil {
		t.Errorf("email filter: %+v", email.Items)
	}
}

func TestCreateNotificationFromTemplate(t *testing.T) {
	s := newServer(t)
	tpl := s.createTemplate(t, "order_shipped", "sms", "Order {{order_id}} shipped, {{name}}!")

	// The channel comes from the template when it is omitted.
	body := fmt.Sprintf(`{"recipient":"+905551234567","templateId":%q,"variables":{"order_id":"A-42","name":"Ayşe"}}`, tpl.ID)
	var n notificationJSON
	s.do(t, http.MethodPost, "/v1/notifications", body).wantStatus(http.StatusAccepted).decode(&n)
	if n.Content != "Order A-42 shipped, Ayşe!" || n.Channel != "sms" {
		t.Errorf("rendered notification: %+v", n)
	}
	if n.TemplateID == nil || *n.TemplateID != tpl.ID || n.TemplateVersion == nil || *n.TemplateVersion != 1 {
		t.Errorf("template ref = %v/%v", n.TemplateID, n.TemplateVersion)
	}
	if ref := s.store.lastCreate.Template; ref == nil || ref.ID != tpl.ID || ref.Version != 1 {
		t.Errorf("store got template ref %+v", ref)
	}

	// The matching channel may be given explicitly.
	s.do(t, http.MethodPost, "/v1/notifications", fmt.Sprintf(
		`{"recipient":"+905551234567","channel":"sms","templateId":%q,"variables":{"order_id":"1","name":"x"}}`, tpl.ID)).
		wantStatus(http.StatusAccepted)
}

func TestCreateNotificationFromTemplateValidation(t *testing.T) {
	s := newServer(t)
	tpl := s.createTemplate(t, "order_shipped", "sms", "Order {{order_id}} shipped, {{name}}!")
	id := tpl.ID.String()
	long := strings.Repeat("x", 1600)

	tests := []struct {
		name, body string
		fields     []string
	}{
		{"missing and unused variables",
			`{"recipient":"+905551234567","templateId":"` + id + `","variables":{"order_id":"1","nme":"x"}}`,
			[]string{"variables.name", "variables.nme"}},
		{"no variables at all",
			`{"recipient":"+905551234567","templateId":"` + id + `"}`,
			[]string{"variables.order_id", "variables.name"}},
		{"content and template",
			`{"recipient":"+905551234567","content":"hi","templateId":"` + id + `","variables":{"order_id":"1","name":"x"}}`,
			[]string{"content"}},
		{"channel differs from the template",
			`{"recipient":"user@example.com","channel":"email","templateId":"` + id + `","variables":{"order_id":"1","name":"x"}}`,
			[]string{"channel"}},
		{"rendered text over the SMS limit",
			`{"recipient":"+905551234567","templateId":"` + id + `","variables":{"order_id":"` + long + `","name":"x"}}`,
			[]string{"content"}},
		{"recipient checked against the template's channel",
			`{"recipient":"user@example.com","templateId":"` + id + `","variables":{"order_id":"1","name":"x"}}`,
			[]string{"recipient"}},
		{"unknown template",
			`{"recipient":"+905551234567","templateId":"01a1fd17-4a0b-7869-8fa8-a36f9d5fb75a","variables":{}}`,
			[]string{"templateId"}},
		{"template id not a UUID",
			`{"recipient":"+905551234567","templateId":"order_shipped"}`,
			[]string{"templateId"}},
		{"variables without a template",
			`{"recipient":"+905551234567","channel":"sms","content":"hi","variables":{"a":"b"}}`,
			[]string{"variables"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertViolations(t, s.do(t, http.MethodPost, "/v1/notifications", tt.body), tt.fields...)
		})
	}
	// A variable value that is not a string is a malformed request.
	s.do(t, http.MethodPost, "/v1/notifications",
		`{"recipient":"+905551234567","templateId":"`+id+`","variables":{"order_id":42,"name":"x"}}`).wantStatus(http.StatusBadRequest)
}

// TestTemplateRequestReplaysAfterTemplateEdit: the idempotency hash covers templateId and
// variables, not the rendered text, so a retry after the template changed replays the original
// notification instead of failing with 409 (ADR-0010).
func TestTemplateRequestReplaysAfterTemplateEdit(t *testing.T) {
	s := newServer(t)
	tpl := s.createTemplate(t, "otp", "sms", "Your code is {{code}}")
	body := fmt.Sprintf(`{"recipient":"+905551234567","templateId":%q,"variables":{"code":"482913"}}`, tpl.ID)

	var first notificationJSON
	s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "otp-1").wantStatus(http.StatusAccepted).decode(&first)
	s.do(t, http.MethodPut, "/v1/templates/"+tpl.ID.String(),
		`{"name":"otp","channel":"sms","body":"Code: {{code}}"}`).wantStatus(http.StatusOK)

	res := s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "otp-1").wantStatus(http.StatusAccepted)
	var again notificationJSON
	res.decode(&again)
	if res.Header().Get("Idempotent-Replayed") != "true" || again.ID != first.ID || again.Content != "Your code is 482913" {
		t.Errorf("retry after the edit: replayed=%q %+v", res.Header().Get("Idempotent-Replayed"), again)
	}

	// An edit that renames the variable, and then a delete, would both fail validation now; the
	// retry still replays without rendering.
	s.do(t, http.MethodPut, "/v1/templates/"+tpl.ID.String(),
		`{"name":"otp","channel":"sms","body":"Code: {{otp}}"}`).wantStatus(http.StatusOK)
	s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "otp-1").wantStatus(http.StatusAccepted)
	s.do(t, http.MethodDelete, "/v1/templates/"+tpl.ID.String(), "").wantStatus(http.StatusNoContent)
	res = s.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "otp-1").wantStatus(http.StatusAccepted)
	res.decode(&again)
	if res.Header().Get("Idempotent-Replayed") != "true" || again.ID != first.ID {
		t.Errorf("retry after the delete: replayed=%q %+v", res.Header().Get("Idempotent-Replayed"), again)
	}

	// Different variables under the same key are a different request.
	s.do(t, http.MethodPost, "/v1/notifications",
		fmt.Sprintf(`{"recipient":"+905551234567","templateId":%q,"variables":{"code":"000000"}}`, tpl.ID),
		"Idempotency-Key", "otp-1").wantStatus(http.StatusConflict)
}

// TestTemplateBatchReplaysAfterTemplateDelete is the batch form of the replay rule: the batch key
// is checked before the templates are loaded.
func TestTemplateBatchReplaysAfterTemplateDelete(t *testing.T) {
	s := newServer(t)
	tpl := s.createTemplate(t, "sale", "sms", "Sale: {{pct}}% off")
	body := fmt.Sprintf(`{"notifications":[{"recipient":"+905551234567","templateId":%q,"variables":{"pct":"20"}}]}`, tpl.ID)
	var first batchCreatedJSON
	s.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "b-1").wantStatus(http.StatusAccepted).decode(&first)
	s.do(t, http.MethodDelete, "/v1/templates/"+tpl.ID.String(), "").wantStatus(http.StatusNoContent)

	res := s.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "b-1").wantStatus(http.StatusAccepted)
	var again batchCreatedJSON
	res.decode(&again)
	if res.Header().Get("Idempotent-Replayed") != "true" || again.Batch.ID != first.Batch.ID {
		t.Errorf("batch retry after the delete: replayed=%q batch %s, want %s", res.Header().Get("Idempotent-Replayed"), again.Batch.ID, first.Batch.ID)
	}
	// The same key on a single create is a different request.
	s.do(t, http.MethodPost, "/v1/notifications",
		fmt.Sprintf(`{"recipient":"+905551234567","templateId":%q,"variables":{"pct":"20"}}`, tpl.ID),
		"Idempotency-Key", "b-1").wantStatus(http.StatusConflict)
}

func TestBatchFromTemplates(t *testing.T) {
	s := newServer(t)
	sms := s.createTemplate(t, "sale_sms", "sms", "Sale: {{pct}}% off")
	email := s.createTemplate(t, "sale_email", "email", "Hi {{name}}, {{pct}}% off today")

	body := fmt.Sprintf(`{"notifications":[
		{"recipient":"+905551234567","templateId":%q,"variables":{"pct":"20"}},
		{"recipient":"user@example.com","templateId":%q,"variables":{"name":"Ali","pct":"20"},"priority":"low"},
		{"recipient":"fcm-token-123456","channel":"push","content":"plain"}]}`, sms.ID, email.ID)
	var res batchCreatedJSON
	s.do(t, http.MethodPost, "/v1/notifications/batch", body).wantStatus(http.StatusAccepted).decode(&res)
	if res.Items[0].Notification.Content != "Sale: 20% off" || res.Items[1].Notification.Content != "Hi Ali, 20% off today" ||
		res.Items[1].Notification.Channel != string(domain.ChannelEmail) || res.Items[2].Notification.TemplateID != nil {
		t.Errorf("batch items: %+v", res.Items)
	}

	bad := fmt.Sprintf(`{"notifications":[
		{"recipient":"+905551234567","templateId":%q,"variables":{"pct":"20"}},
		{"recipient":"+905551234567","templateId":%q,"variables":{}}]}`, sms.ID, sms.ID)
	assertViolations(t, s.do(t, http.MethodPost, "/v1/notifications/batch", bad), "notifications[1].variables.pct")
}

// assertViolations checks a 422 validation_failed response lists exactly fields.
func assertViolations(t *testing.T, res response, fields ...string) {
	t.Helper()
	e := res.wantStatus(http.StatusUnprocessableEntity).errorBody()
	var got []string
	for _, d := range e.Details {
		m, _ := d.(map[string]any)
		f, _ := m["field"].(string)
		got = append(got, f)
	}
	slices.Sort(got)
	want := slices.Clone(fields)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("violations on %v, want %v (%s)", got, want, res.Body.String())
	}
}

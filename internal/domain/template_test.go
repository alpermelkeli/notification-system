package domain

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestValidateTemplateName(t *testing.T) {
	for _, name := range []string{"order_shipped", "otp.v2", "a", "2fa-code", strings.Repeat("a", 100)} {
		if err := ValidateTemplateName(name); err != nil {
			t.Errorf("%q: unexpected error %v", name, err)
		}
	}
	for _, name := range []string{"", "Order", "_x", "-x", "has space", "ünicode", strings.Repeat("a", 101)} {
		if err := ValidateTemplateName(name); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: got %v, want ErrInvalidArgument", name, err)
		}
	}
}

func TestValidateTemplateBody(t *testing.T) {
	tests := []struct {
		body    string
		vars    []string
		wantErr string
	}{
		{body: "Hi {{name}}, order {{ order_id }} ships {{name}}", vars: []string{"name", "order_id"}},
		{body: "no placeholders", vars: nil},
		{body: "a lone }} is literal", vars: nil},
		{body: "{{_x1}}", vars: []string{"_x1"}},
		{body: "Hi {{name", wantErr: "unclosed placeholder at byte 3"},
		{body: "Hi {{}}", wantErr: "malformed placeholder at byte 3"},
		{body: "Hi {{first name}}", wantErr: "malformed placeholder"},
		{body: "Hi {{1x}}", wantErr: "malformed placeholder"},
		{body: "{{" + strings.Repeat("v", 65) + "}}", wantErr: "longer than 64"},
		{body: "   ", wantErr: "is required"},
	}
	for _, tt := range tests {
		vars, err := ValidateTemplateBody(ChannelSMS, tt.body)
		if tt.wantErr != "" {
			if err == nil || !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("%q: got %v, want an error containing %q", tt.body, err, tt.wantErr)
			}
			continue
		}
		if err != nil || !slices.Equal(vars, tt.vars) {
			t.Errorf("%q: got %v, %v; want %v", tt.body, vars, err, tt.vars)
		}
	}
}

func TestValidateTemplateBodyLimits(t *testing.T) {
	var many strings.Builder
	for i := range MaxTemplateVariables + 1 {
		fmt.Fprintf(&many, "{{v%d}}", i)
	}
	if _, err := ValidateTemplateBody(ChannelEmail, many.String()); err == nil || !strings.Contains(err.Error(), "more than 50") {
		t.Errorf("51 variables: got %v", err)
	}
	// The body as written must fit the channel: 1531 GSM-7 characters need 11 segments.
	if _, err := ValidateTemplateBody(ChannelSMS, strings.Repeat("x", 1531)); err == nil || !strings.Contains(err.Error(), "SMS segments") {
		t.Errorf("oversized SMS body: got %v", err)
	}
}

func TestTemplateRender(t *testing.T) {
	tpl := Template{Body: "Hi {{name}}, order {{ order_id }} is on its way. Thanks, {{name}}!"}
	got, err := tpl.Render(map[string]string{"name": "Ayşe", "order_id": "A-42"})
	if err != nil || got != "Hi Ayşe, order A-42 is on its way. Thanks, Ayşe!" {
		t.Fatalf("got %q, %v", got, err)
	}
	if v := tpl.Variables(); !slices.Equal(v, []string{"name", "order_id"}) {
		t.Errorf("Variables() = %v", v)
	}

	// Values are inserted as they are: a value that looks like a placeholder is not expanded.
	got, err = tpl.Render(map[string]string{"name": "{{order_id}}", "order_id": ""})
	if err != nil || got != "Hi {{order_id}}, order  is on its way. Thanks, {{order_id}}!" {
		t.Errorf("got %q, %v", got, err)
	}

	_, err = tpl.Render(map[string]string{"name": "x", "orderId": "1", "extra": "2"})
	var re *RenderError
	if !errors.As(err, &re) || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v, want *RenderError", err)
	}
	if !slices.Equal(re.Missing, []string{"order_id"}) || !slices.Equal(re.Unused, []string{"extra", "orderId"}) {
		t.Errorf("missing=%v unused=%v", re.Missing, re.Unused)
	}

	if got, err := (Template{Body: "static"}).Render(nil); err != nil || got != "static" {
		t.Errorf("static template: %q, %v", got, err)
	}
}

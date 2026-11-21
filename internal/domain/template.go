package domain

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Template limits (B-3, ADR-0010).
const (
	MaxTemplateVariables     = 50
	MaxTemplateVariableBytes = 64
)

var (
	templateName = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,99}$`)
	variableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// Template is a stored message body with {{variable}} placeholders (B-3). Notifications created
// from it store the rendered text, so editing a template never changes an accepted notification
// (ADR-0010).
type Template struct {
	ID      uuid.UUID
	Name    string
	Channel Channel
	Body    string
	// Version starts at 1 and is incremented by every update.
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ValidateTemplateName checks a template name: lower-case letters, digits, '_', '.' and '-',
// starting with a letter or digit, at most 100 characters.
func ValidateTemplateName(name string) error {
	if name == "" {
		return invalid("is required")
	}
	if !templateName.MatchString(name) {
		return invalid("must be 1 to 100 characters of a-z, 0-9, '_', '.' or '-', starting with a letter or digit")
	}
	return nil
}

// ValidateTemplateBody checks a template body for ch: its placeholders must be well formed and
// the body, with each placeholder counted as one character, must pass the channel's content
// rules. It returns the variables in order of
// first use.
func ValidateTemplateBody(ch Channel, body string) ([]string, error) {
	segs, err := parseTemplate(body)
	if err != nil {
		return nil, err
	}
	// Check the text with each placeholder as one character: "{" and "}" are GSM-7 extension
	// characters that cost two septets each, so the body as written would overcount. The rendered
	// text is checked again at creation.
	var b strings.Builder
	for _, s := range segs {
		if s.variable {
			b.WriteByte('x')
		} else {
			b.WriteString(s.text)
		}
	}
	if err := ValidateContent(ch, b.String()); err != nil {
		return nil, err
	}
	return variablesOf(segs), nil
}

// Variables returns the variables the template uses, in order of first use. The body was
// validated when the template was stored; a malformed body yields no variables.
func (t Template) Variables() []string {
	segs, err := parseTemplate(t.Body)
	if err != nil {
		return nil
	}
	return variablesOf(segs)
}

// RenderError lists the variables a render was missing and those it was given but the template
// does not use. It matches ErrInvalidArgument with errors.Is.
type RenderError struct {
	Missing []string
	Unused  []string
}

func (e *RenderError) Error() string {
	var parts []string
	if len(e.Missing) > 0 {
		parts = append(parts, "missing variables: "+strings.Join(e.Missing, ", "))
	}
	if len(e.Unused) > 0 {
		parts = append(parts, "unused variables: "+strings.Join(e.Unused, ", "))
	}
	return fmt.Sprintf("%s: %s", ErrInvalidArgument, strings.Join(parts, "; "))
}

// Unwrap lets errors.Is(err, ErrInvalidArgument) succeed.
func (e *RenderError) Unwrap() error { return ErrInvalidArgument }

// Render substitutes vars into the template. Every variable of the template must be given and
// every given variable must be used, so a typo in a name is an error rather than an empty or
// ignored value (*RenderError). The result is not validated against the channel's rules.
func (t Template) Render(vars map[string]string) (string, error) {
	segs, err := parseTemplate(t.Body)
	if err != nil {
		return "", err
	}
	used := variablesOf(segs)
	rerr := &RenderError{}
	for _, v := range used {
		if _, ok := vars[v]; !ok {
			rerr.Missing = append(rerr.Missing, v)
		}
	}
	for v := range vars {
		if !slices.Contains(used, v) {
			rerr.Unused = append(rerr.Unused, v)
		}
	}
	if len(rerr.Missing) > 0 || len(rerr.Unused) > 0 {
		slices.Sort(rerr.Unused) // map order is random
		return "", rerr
	}
	var b strings.Builder
	for _, s := range segs {
		if s.variable {
			b.WriteString(vars[s.text])
		} else {
			b.WriteString(s.text)
		}
	}
	return b.String(), nil
}

// segment is literal text or a variable reference.
type segment struct {
	text     string
	variable bool
}

// parseTemplate splits body into literal text and {{ name }} placeholders. Every "{{" must open a
// well-formed placeholder; a lone "}}" is literal text.
func parseTemplate(body string) ([]segment, error) {
	var segs []segment
	seen := map[string]bool{}
	rest, offset := body, 0
	for {
		open := strings.Index(rest, "{{")
		if open < 0 {
			if rest != "" {
				segs = append(segs, segment{text: rest})
			}
			return segs, nil
		}
		if open > 0 {
			segs = append(segs, segment{text: rest[:open]})
		}
		end := strings.Index(rest[open+2:], "}}")
		if end < 0 {
			return nil, invalid(fmt.Sprintf("has an unclosed placeholder at byte %d", offset+open))
		}
		name := strings.TrimSpace(rest[open+2 : open+2+end])
		switch {
		case !variableName.MatchString(name):
			return nil, invalid(fmt.Sprintf("has a malformed placeholder at byte %d: a variable name is a letter or '_' followed by letters, digits or '_'", offset+open))
		case len(name) > MaxTemplateVariableBytes:
			return nil, invalid(fmt.Sprintf("has a variable name longer than %d characters at byte %d", MaxTemplateVariableBytes, offset+open))
		}
		if !seen[name] {
			if len(seen) == MaxTemplateVariables {
				return nil, invalid(fmt.Sprintf("uses more than %d variables", MaxTemplateVariables))
			}
			seen[name] = true
		}
		segs = append(segs, segment{text: name, variable: true})
		consumed := open + 2 + end + 2
		rest, offset = rest[consumed:], offset+consumed
	}
}

func variablesOf(segs []segment) []string {
	var out []string
	for _, s := range segs {
		if s.variable && !slices.Contains(out, s.text) {
			out = append(out, s.text)
		}
	}
	return out
}

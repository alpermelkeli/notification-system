// Package provider is the HTTP client of the external delivery provider (webhook.site in the
// brief, the mock provider locally). It speaks the contract
// POST {to, channel, content} -> 202 {messageId, status, timestamp} and reports non-2xx answers
// as *Error, so the worker can classify them (ADR-0005).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/alpermelkeli/notification-system/internal/domain"
	"github.com/alpermelkeli/notification-system/internal/telemetry"
)

// Request headers sent with every delivery.
const (
	// HeaderIdempotencyKey lets the provider deduplicate a retry of a request whose response we
	// lost (ADR-0004). It is the notification id, so it is stable across attempts.
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderCorrelationID  = "X-Correlation-ID"
)

// maxBody bounds how much of a response is read.
const maxBody = 64 << 10

// Message is one delivery request.
type Message struct {
	IdempotencyKey string
	CorrelationID  string
	To             string
	Channel        domain.Channel
	Content        string
}

// Result is the provider's answer to an accepted delivery.
type Result struct {
	// MessageID is the provider's id of the message, if it returned one.
	MessageID string
}

// Error is a non-2xx answer from the provider.
type Error struct {
	StatusCode int
	// RetryAfter is the parsed Retry-After header, or 0 if there was none.
	RetryAfter time.Duration
	// Body is the start of the response body, for last_error and logs.
	Body string
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("provider returned %d", e.StatusCode)
	if e.Body != "" {
		msg += ": " + e.Body
	}
	return msg
}

// Client sends messages to one provider endpoint. It is safe for concurrent use.
type Client struct {
	url  string
	name string
	http *http.Client
}

// New returns a client for the provider at rawURL. timeout bounds each request, including
// reading the response.
func New(rawURL string, timeout time.Duration) (*Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("provider URL %q must be an absolute http(s) URL", rawURL)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Up to WORKER_CONCURRENCY sends per channel run at once against a single host.
	transport.MaxIdleConnsPerHost = 100
	return &Client{
		url:  rawURL,
		name: u.Host,
		http: &http.Client{Timeout: timeout, Transport: transport},
	}, nil
}

// Name identifies the provider endpoint (its host). Rate-limit keys are per provider (ADR-0006).
func (c *Client) Name() string { return c.name }

type requestBody struct {
	To      string         `json:"to"`
	Channel domain.Channel `json:"channel"`
	Content string         `json:"content"`
}

type responseBody struct {
	MessageID string `json:"messageId"`
}

// Send delivers m. It returns *Error for a non-2xx answer and a wrapped transport error (timeout,
// connection refused, ...) when no answer arrived.
//
// Each call gets a client span and sends its traceparent (B-5). The span names only the provider
// host: the URL path is a credential (the webhook.site inbox id), so it never reaches traces,
// logs or last_error.
func (c *Client) Send(ctx context.Context, m Message) (res Result, err error) {
	ctx, span := telemetry.Tracer().Start(ctx, "POST "+c.name, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(semconv.HTTPRequestMethodPost, semconv.ServerAddress(c.name)))
	defer func() {
		var pe *Error
		switch {
		case errors.As(err, &pe):
			span.SetStatus(codes.Error, http.StatusText(pe.StatusCode))
		case err != nil:
			span.RecordError(err)
			span.SetStatus(codes.Error, "no answer")
		}
		span.End()
	}()

	body, err := json.Marshal(requestBody{To: m.To, Channel: m.Channel, Content: m.Content})
	if err != nil {
		return Result{}, fmt.Errorf("encode provider request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("build provider request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if m.IdempotencyKey != "" {
		req.Header.Set(HeaderIdempotencyKey, m.IdempotencyKey)
	}
	if m.CorrelationID != "" {
		req.Header.Set(HeaderCorrelationID, m.CorrelationID)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := c.http.Do(req)
	if err != nil {
		// *url.Error quotes the full URL; keep only its cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return Result{}, fmt.Errorf("call provider %s: %w", c.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	span.SetAttributes(semconv.HTTPResponseStatusCode(resp.StatusCode))
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return Result{}, fmt.Errorf("read provider response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return Result{}, &Error{
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
			Body:       snippet(raw),
		}
	}
	// An accepted message is delivered even if the body is not what we expect; the id is only
	// informational, so a malformed body must not cause a duplicate send.
	var rb responseBody
	_ = json.Unmarshal(raw, &rb)
	return Result(rb), nil
}

// parseRetryAfter reads a Retry-After value in seconds or as an HTTP date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(max(secs, 0)) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0)
	}
	return 0
}

func snippet(b []byte) string {
	const maxLen = 200
	s := strings.TrimSpace(string(b))
	if len(s) > maxLen {
		s = strings.TrimSpace(s[:maxLen])
	}
	return s
}

// IsTimeout reports whether err is a request that timed out without an answer.
func IsTimeout(err error) bool {
	var ne interface{ Timeout() bool }
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

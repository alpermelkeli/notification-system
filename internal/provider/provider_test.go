package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestSendAccepted(t *testing.T) {
	var got struct {
		body                 requestBody
		key, corr, ctype, at string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.key, got.corr, got.ctype = r.Header.Get(HeaderIdempotencyKey), r.Header.Get(HeaderCorrelationID), r.Header.Get("Content-Type")
		got.at = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&got.body); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-1","status":"accepted","timestamp":"2026-11-21T10:00:00Z"}`))
	}))
	defer srv.Close()

	c, err := New(srv.URL+"/hook-id", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Send(context.Background(), Message{
		IdempotencyKey: "id-1", CorrelationID: "corr-1", To: "+905551112233", Channel: domain.ChannelSMS, Content: "hi",
	})
	if err != nil || res.MessageID != "m-1" {
		t.Fatalf("Send = %+v, %v", res, err)
	}
	if got.body != (requestBody{To: "+905551112233", Channel: domain.ChannelSMS, Content: "hi"}) ||
		got.key != "id-1" || got.corr != "corr-1" || got.ctype != "application/json" || got.at != "/hook-id" {
		t.Errorf("request = %+v", got)
	}
	if c.Name() != strings.TrimPrefix(srv.URL, "http://") {
		t.Errorf("Name = %q", c.Name())
	}
}

func TestSendAcceptedWithUnexpectedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, time.Second)
	if res, err := c.Send(context.Background(), Message{To: "x", Channel: domain.ChannelPush, Content: "y"}); err != nil || res.MessageID != "" {
		t.Fatalf("a 2xx must count as delivered: %+v, %v", res, err)
	}
}

func TestSendErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/429":
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
		case "/400":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid recipient"}` + strings.Repeat(" ", 500) + "tail"))
		case "/slow":
			time.Sleep(200 * time.Millisecond)
		}
	}))
	defer srv.Close()

	c, _ := New(srv.URL+"/429", time.Second)
	_, err := c.Send(context.Background(), Message{})
	var pe *Error
	if !errors.As(err, &pe) || pe.StatusCode != http.StatusTooManyRequests || pe.RetryAfter != 3*time.Second {
		t.Fatalf("429: %v", err)
	}

	c, _ = New(srv.URL+"/400", time.Second)
	_, err = c.Send(context.Background(), Message{})
	if !errors.As(err, &pe) || pe.StatusCode != http.StatusBadRequest || pe.Body != `{"error":"invalid recipient"}` {
		t.Fatalf("400: %v (body %q)", err, pe.Body)
	}

	c, _ = New(srv.URL+"/slow", 50*time.Millisecond)
	_, err = c.Send(context.Background(), Message{})
	if err == nil || errors.As(err, &pe) || !IsTimeout(err) {
		t.Fatalf("timeout: %v", err)
	}

	srv.Close()
	c, _ = New(srv.URL, time.Second)
	if _, err := c.Send(context.Background(), Message{}); err == nil || IsTimeout(err) {
		t.Fatalf("connection refused: %v", err)
	}
}

func TestNewRejectsBadURLs(t *testing.T) {
	for _, u := range []string{"", "webhook.site/x", "ftp://host/x", "http://", "://bad"} {
		if _, err := New(u, time.Second); err == nil {
			t.Errorf("New(%q) accepted", u)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":                              0,
		"5":                             5 * time.Second,
		"-1":                            0,
		"soon":                          0,
		"Sat, 21 Nov 2026 10:00:07 GMT": 7 * time.Second,
		"Sat, 21 Nov 2026 09:00:00 GMT": 0,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}

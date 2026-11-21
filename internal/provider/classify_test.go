package provider

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"nil", nil, ClassDelivered},
		{"timeout", fmt.Errorf("call provider: %w", context.DeadlineExceeded), ClassRetryable},
		{"connection refused", errors.New("dial tcp: connection refused"), ClassRetryable},
		{"500", &Error{StatusCode: 500}, ClassRetryable},
		{"503", &Error{StatusCode: 503}, ClassRetryable},
		{"408", &Error{StatusCode: 408}, ClassRetryable},
		{"425", &Error{StatusCode: 425}, ClassRetryable},
		{"429", &Error{StatusCode: 429}, ClassRateLimited},
		{"wrapped 429", fmt.Errorf("send: %w", &Error{StatusCode: 429}), ClassRateLimited},
		{"400", &Error{StatusCode: 400}, ClassPermanent},
		{"401", &Error{StatusCode: 401}, ClassPermanent},
		{"404", &Error{StatusCode: 404}, ClassPermanent},
		{"422", &Error{StatusCode: 422}, ClassPermanent},
		{"302", &Error{StatusCode: 302}, ClassRetryable},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("%s: Classify = %s, want %s", c.name, got, c.want)
		}
	}
}

package provider

import (
	"errors"
	"net/http"
)

// Class is how a delivery outcome is handled (ADR-0005, FR-3.2).
type Class int

// Delivery outcome classes.
const (
	// ClassDelivered is a 2xx answer.
	ClassDelivered Class = iota
	// ClassRetryable is a failure that may succeed later: no answer (timeout, connection error),
	// 5xx, 408 or 425. It counts against the circuit breaker.
	ClassRetryable
	// ClassRateLimited is a 429: the provider is healthy but asks us to slow down. It is retried
	// after max(backoff, Retry-After), pauses the whole fleet and is not counted by the breaker.
	ClassRateLimited
	// ClassPermanent is any other 4xx: the request itself is wrong, so retrying cannot help.
	ClassPermanent
)

func (c Class) String() string {
	switch c {
	case ClassDelivered:
		return "delivered"
	case ClassRetryable:
		return "retryable"
	case ClassRateLimited:
		return "rate_limited"
	case ClassPermanent:
		return "permanent"
	}
	return "unknown"
}

// Classify returns the class of the error Send returned. A nil error is ClassDelivered. Any error
// that is not an *Error means no answer arrived and is retryable.
func Classify(err error) Class {
	if err == nil {
		return ClassDelivered
	}
	var pe *Error
	if !errors.As(err, &pe) {
		return ClassRetryable
	}
	switch code := pe.StatusCode; {
	case code >= 200 && code <= 299:
		return ClassDelivered
	case code == http.StatusTooManyRequests:
		return ClassRateLimited
	case code == http.StatusRequestTimeout, code == http.StatusTooEarly, code >= 500:
		return ClassRetryable
	case code >= 400:
		return ClassPermanent
	}
	// 1xx and 3xx should never reach us (redirects are followed); treat them as transient.
	return ClassRetryable
}

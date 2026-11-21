// Package httpx holds HTTP plumbing shared by every server in the binary: JSON responses, the
// error envelope and middleware (correlation ID, instance ID, request logging, panic recovery).
package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// ErrorBody is the single error envelope used by every endpoint (docs/conventions.md).
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes an error. Details is optional and holds e.g. field violations.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details []any  `json:"details,omitempty"`
}

// WriteJSON writes v as a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already sent; all we can do is record the failure.
		slog.Default().Debug("write json response", "err", err)
	}
}

// WriteError writes the error envelope.
func WriteError(w http.ResponseWriter, status int, code, message string, details ...any) {
	WriteJSON(w, status, ErrorBody{Error: ErrorDetail{Code: code, Message: message, Details: details}})
}

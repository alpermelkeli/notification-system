// Package apispec embeds the OpenAPI contract of the public API (D-4). The spec is written first
// and the handlers in internal/api follow it; a test keeps the routes and the spec in sync.
package apispec

import _ "embed"

// OpenAPI is the OpenAPI 3.1 document, served at /openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPI []byte

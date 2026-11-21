// Package migrations embeds the versioned SQL migrations (D-5). They are applied by
// `notifier migrate` through pressly/goose (ADR-0007).
//
// Rules: files are named NNNNN_description.sql, are forward-only and are never edited after
// they have been merged.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS

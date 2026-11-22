# Engineering Conventions

## Go
- Go 1.2x (target the version pinned in `go.mod`). `gofmt` + `go vet` + `golangci-lint` must pass.
- Standard project layout: `cmd/<binary>/main.go`, `internal/...` for non-exported packages. No `pkg/` unless something is truly reusable.
- Depend on interfaces at the consumer side; keep them small. Constructors take dependencies explicitly (no globals, no `init()` side effects).
- `context.Context` is the first parameter of anything doing I/O; respect cancellation and deadlines.
- Errors: wrap with `fmt.Errorf("doing x: %w", err)`; define sentinel / typed errors in the domain; map to HTTP status only at the transport layer.
- Logging: `log/slog` JSON handler; every log line in a request/job carries `correlation_id`. Never log secrets or full message content at info level.
- Config: environment variables only, parsed once at startup into a typed struct; document every variable in `docs/configuration.md` + `.env.example`.
- Graceful shutdown on SIGINT/SIGTERM for HTTP server and workers.

## Testing
- Table-driven unit tests next to the code (`foo_test.go`).
- Integration tests use real dependencies via docker (testcontainers or compose), guarded by a build tag or `-short`.
- External provider is always faked in tests (httptest server). No test may call webhook.site.
- `make test` runs everything; must be green before every commit.

## API
- JSON over HTTP, versioned under `/v1`. Async operations return `202 Accepted`.
- Errors use a single envelope: `{ "error": { "code": "...", "message": "...", "details": [...] } }`.
- Cursor-based pagination for list endpoints.
- OpenAPI spec is the contract — keep it in sync in the same commit as handler changes.

## Database
- Migrations are versioned, forward-only, never edited after merge.
- Every state transition is a guarded `UPDATE ... WHERE status = <expected>` (optimistic concurrency).

## Git
- Conventional Commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`, `ci:`, `build:`.
- Small, focused commits — reviewers will read the history. One logical change per commit, each one builds and passes tests.
- Branch per phase/feature, squash-free merges so history stays readable.

## Architecture decisions
- Every significant decision gets an ADR in `docs/adr/NNNN-title.md` using `0000-template.md`.
- The README's "How it works" section summarizes the key ADRs; `docs/how-it-works.md` lists them all.

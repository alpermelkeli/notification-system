package api

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	apispec "github.com/alpermelkeli/notification-system/api"
)

// TestRoutesMatchOpenAPISpec keeps the contract and the router in sync: every /v1 route is
// documented and every documented /v1 operation is routed. The spec itself is linted with
// Redocly in CI.
func TestRoutesMatchOpenAPISpec(t *testing.T) {
	r := chi.NewRouter()
	New(newFakeStore(), slog.New(slog.NewTextHandler(io.Discard, nil)), Options{}).Routes(r)
	var routed []string
	if err := chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if strings.HasPrefix(route, "/v1/") {
			routed = append(routed, method+" "+route)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	pathLine := regexp.MustCompile(`^  (/\S+):\s*$`)
	opLine := regexp.MustCompile(`^    (get|post|put|patch|delete):\s*$`)
	var documented []string
	path := ""
	sc := bufio.NewScanner(bytes.NewReader(apispec.OpenAPI))
	for sc.Scan() {
		line := sc.Text()
		if m := pathLine.FindStringSubmatch(line); m != nil {
			path = m[1]
			continue
		}
		if line != "" && !strings.HasPrefix(line, "  ") {
			path = "" // left the paths section
		}
		if m := opLine.FindStringSubmatch(line); m != nil && strings.HasPrefix(path, "/v1/") {
			documented = append(documented, strings.ToUpper(m[1])+" "+path)
		}
	}
	slices.Sort(routed)
	slices.Sort(documented)
	if len(documented) == 0 || !slices.Equal(routed, documented) {
		t.Errorf("routes and spec differ\nrouted:     %v\ndocumented: %v", routed, documented)
	}
}

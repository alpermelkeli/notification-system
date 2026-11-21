package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

func TestIsTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, false},
		{"deadline", fmt.Errorf("claim: %w", context.DeadlineExceeded), true},
		{"connection refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"unexpected eof", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"admin shutdown", &pgconn.PgError{Code: "57P01"}, true},
		{"cannot connect now", &pgconn.PgError{Code: "57P03"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"serialization failure", &pgconn.PgError{Code: "40001"}, true},
		{"lock not available", &pgconn.PgError{Code: "55P03"}, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"syntax error", &pgconn.PgError{Code: "42601"}, false},
		{"lease lost", domain.ErrLeaseLost, false},
		{"transition", &domain.TransitionError{Current: domain.StatusSent, Target: domain.StatusProcessing}, false},
		{"not found", domain.ErrNotFound, false},
	}
	for _, c := range cases {
		if got := IsTransient(c.err); got != c.want {
			t.Errorf("%s: IsTransient = %v, want %v", c.name, got, c.want)
		}
	}
}

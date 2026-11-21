package storage

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsTransient reports whether err means PostgreSQL was unavailable rather than that the statement
// was wrong: no connection, a lost connection, a timeout, the server shutting down or out of
// resources, or a serialization failure. Retrying the same statement later may succeed. A guard
// failure (domain.ErrLeaseLost, *domain.TransitionError) or a constraint violation is not
// transient.
func IsTransient(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:2] {
		case "08", // connection exception
			"40", // transaction rollback: serialization failure, deadlock
			"53", // insufficient resources: too many connections, disk full, out of memory
			"57", // operator intervention: admin shutdown, crash shutdown, cannot connect now
			"58": // system error: I/O error
			return true
		}
		return pgErr.Code == "55P03" // lock not available
	}
	var (
		connErr *pgconn.ConnectError
		netErr  net.Error
	)
	// context.DeadlineExceeded is a net.Error with Timeout() true; pgconn reports a closed or busy
	// connection as SafeToRetry.
	return errors.As(err, &connErr) || errors.As(err, &netErr) || pgconn.Timeout(err) || pgconn.SafeToRetry(err) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

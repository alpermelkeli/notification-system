package dispatcher

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/alpermelkeli/notification-system/internal/testinfra"
)

// kafkaAddr is a Kafka broker shared by the integration tests, or "" under -short.
var kafkaAddr string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	addr, stop, err := testinfra.Kafka(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	kafkaAddr = addr
	code := m.Run()
	stop()
	os.Exit(code)
}

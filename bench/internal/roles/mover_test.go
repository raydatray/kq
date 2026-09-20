package roles

import (
	"context"
	"strings"
	"testing"

	"github.com/raydatray/kq/bench/internal/events"
	"io"
)

func TestRunMoverRejectsInvalidGrid(t *testing.T) {
	config := testProducerConfig()
	config.KQ.RetryGridBoundariesMS = []int64{0} // invalid: needs two boundaries
	output := events.NewWriter(io.Discard)
	defer func() {
		if err := output.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	err := RunMover(context.Background(), config, output)
	if err == nil {
		t.Fatal("RunMover = nil, want grid error")
	}
	if !strings.Contains(err.Error(), "retry grid") {
		t.Fatalf("RunMover error = %v, want retry grid", err)
	}
}

func TestRunMoverRejectsEmptyBrokers(t *testing.T) {
	config := testProducerConfig()
	config.KQ.Brokers = nil
	output := events.NewWriter(io.Discard)
	defer func() {
		if err := output.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	if err := RunMover(context.Background(), config, output); err == nil {
		t.Fatal("RunMover = nil, want brokers error")
	}
}

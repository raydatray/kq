package roles

import (
	"context"

	"github.com/raydatray/kq"
	"github.com/raydatray/kq/bench/internal/events"
)

// RunMover returns eligible retries to the ready topic until ctx ends.
//
// The serial mover reports lifecycle events only; per-move throughput is
// derived in the harness from handler outcomes (see kqbench accounting) to
// avoid coupling KQ internals to load instrumentation.
func RunMover(ctx context.Context, config Config, output *events.Writer) error {
	kqConfig, err := config.KQ.ToKQConfig()
	if err != nil {
		return err
	}
	mover, err := kq.NewRetryMover(kqConfig)
	if err != nil {
		return err
	}
	defer mover.Close()

	_ = output.Write(events.ProcessReady(events.RoleMover, config.ProcessIndex))
	err = mover.Run(ctx)
	_ = output.Write(events.ProcessStopped(events.RoleMover, config.ProcessIndex, err))
	return err
}

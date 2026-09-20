package roles

import (
	"context"

	"github.com/raydatray/kq"
	"github.com/raydatray/kq/bench/internal/events"
)

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

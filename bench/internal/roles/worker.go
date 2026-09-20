package roles

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/raydatray/kq"
	"github.com/raydatray/kq/bench/internal/events"
	"github.com/raydatray/kq/bench/internal/workload"
)

func RunWorker(ctx context.Context, config Config, output *events.Writer) error {
	kqConfig, err := config.KQ.ToKQConfig()
	if err != nil {
		return err
	}
	worker, err := kq.NewWorker(kq.WorkerConfig{
		Config:      kqConfig,
		Concurrency: config.Topology.WorkerConcurrency,
	}, func(handlerCtx context.Context, task kq.Task) error {
		return handle(handlerCtx, config.ProcessIndex, task, output)
	})
	if err != nil {
		return err
	}
	defer worker.Close()

	_ = output.Write(events.ProcessReady(events.RoleWorker, config.ProcessIndex))
	err = worker.Run(ctx)
	_ = output.Write(events.ProcessStopped(events.RoleWorker, config.ProcessIndex, err))
	return err
}

func handle(ctx context.Context, process int, task kq.Task, output *events.Writer) error {
	var job workload.Job
	if err := json.Unmarshal(task.Payload, &job); err != nil {
		_ = output.Write(events.ProcessError(events.RoleWorker, process, err))
		return err
	}

	_ = output.Write(events.HandlerStarted(job.WorkloadID, process))
	started := time.Now()

	if err := sleep(ctx, job.SimulatedExecutionTimeMS); err != nil {
		_ = output.Write(events.HandlerFinished(job.WorkloadID, process, started, err))
		return err
	}

	err := applyFailureBehavior(job, time.Now())
	_ = output.Write(events.HandlerFinished(job.WorkloadID, process, started, err))
	return err
}

func sleep(ctx context.Context, ms int64) error {
	if ms <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func applyFailureBehavior(job workload.Job, now time.Time) error {
	switch job.FailureMode {
	case "", workload.FailureModeNone:
		return nil
	case workload.FailureModePermanent:
		return errors.New("bench: permanent failure")
	case workload.FailureModeUntilTime:
		if now.Before(job.FailUntil) {
			return errors.New("bench: outage failure")
		}
		return nil
	default:
		return fmt.Errorf("bench: unknown failure mode %q", job.FailureMode)
	}
}

package roles

import (
	"context"
	"encoding/json"
	"time"

	"github.com/raydatray/kq"
	"github.com/raydatray/kq/bench/internal/events"
	"github.com/raydatray/kq/bench/internal/workload"
)

// RunProducer enqueues its deterministic workload ID range at the requested
// global rate.
func RunProducer(ctx context.Context, config Config, output *events.Writer) error {
	kqConfig, err := config.KQ.ToKQConfig()
	if err != nil {
		return err
	}
	client, err := kq.NewClient(kqConfig)
	if err != nil {
		return err
	}
	defer client.Close()

	_ = output.Write(events.ProcessReady(events.RoleProducer, config.ProcessIndex))
	started := time.Now()
	workloadConfig := config.ToWorkloadConfig()

	// Manual runs without a harness-anchored outage timestamp fall back to
	// producer start plus the configured outage duration.
	if workloadConfig.Failures.Mode == "until_time" && workloadConfig.Failures.Until.IsZero() {
		workloadConfig.Failures.Until = started.Add(
			time.Duration(workloadConfig.Failures.DurationMS) * time.Millisecond,
		)
	}

	total := config.TotalTasks()
	for id := uint64(config.ProcessIndex); id < total; id += uint64(config.Topology.Producers) {
		if err := waitUntil(ctx, config.DueAt(started, id)); err != nil {
			_ = output.Write(events.ProcessStopped(events.RoleProducer, config.ProcessIndex, err))
			return err
		}

		job := workload.Generate(workloadConfig, id)
		job.EnqueuedAt = time.Now()
		payload, err := json.Marshal(job)
		if err != nil {
			return err
		}

		enqueueStarted := time.Now()
		_, err = client.Enqueue(ctx, kq.Task{Type: "bench", Payload: payload})
		_ = output.Write(events.EnqueueFinished(id, config.ProcessIndex, enqueueStarted, err))
		if ctx.Err() != nil {
			_ = output.Write(events.ProcessStopped(events.RoleProducer, config.ProcessIndex, ctx.Err()))
			return ctx.Err()
		}
	}

	_ = output.Write(events.ProcessStopped(events.RoleProducer, config.ProcessIndex, nil))
	return nil
}

func waitUntil(ctx context.Context, dueAt time.Time) error {
	delay := time.Until(dueAt)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

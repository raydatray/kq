package roles

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/raydatray/kq/bench/internal/events"
	"github.com/raydatray/kq/bench/internal/workload"
	kqpb "github.com/raydatray/kq/internal/proto/kq"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func RunObserver(ctx context.Context, config Config, output *events.Writer) error {
	if len(config.KQ.Brokers) == 0 {
		return errors.New("roles: at least one broker is required")
	}
	if config.KQ.Queue == "" {
		return errors.New("roles: queue name cannot be empty")
	}
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(config.KQ.Brokers...),
		kgo.ConsumeTopics(config.KQ.Queue+"-dlq"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer consumer.Close()

	_ = output.Write(events.ProcessReady(events.RoleObserver, 0))

	for {
		fetches := consumer.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			_ = output.Write(events.ProcessStopped(events.RoleObserver, 0, nil))
			return nil
		}
		if fetches.IsClientClosed() {
			_ = output.Write(events.ProcessStopped(events.RoleObserver, 0, nil))
			return nil
		}

		for _, record := range fetches.Records() {
			workloadID, err := workloadIDFromDLQRecord(record.Value)
			if err != nil {
				_ = output.Write(events.ProcessError(events.RoleObserver, 0, err))
				continue
			}
			_ = output.Write(events.DeadLettered(workloadID, time.Now()))
		}

		if err := fetches.Err(); err != nil {
			_ = output.Write(events.ProcessStopped(events.RoleObserver, 0, err))
			return err
		}
	}
}

func workloadIDFromDLQRecord(value []byte) (uint64, error) {
	var envelope kqpb.TaskEnvelope
	if err := proto.Unmarshal(value, &envelope); err != nil {
		return 0, err
	}
	var job workload.Job
	if err := json.Unmarshal(envelope.Payload, &job); err != nil {
		return 0, err
	}
	return job.WorkloadID, nil
}

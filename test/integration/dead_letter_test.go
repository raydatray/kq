//go:build integration

package integration_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/raydatray/kq"
	kqpb "github.com/raydatray/kq/internal/proto/kq"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestExhaustedTaskMovesToDLQ(t *testing.T) {
	broker := os.Getenv("KQ_TEST_BROKERS")
	if broker == "" {
		t.Skip("KQ_TEST_BROKERS is not set")
	}

	config := kq.Config{
		Brokers: []string{broker},
		Queue:   "dlq-test",
	}
	testCtx, cancelTest := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelTest()
	workerCtx, cancelWorker := context.WithCancel(testCtx)
	defer cancelWorker()

	cause := errors.New("permanent failure")
	worker, err := kq.NewWorker(kq.WorkerConfig{Config: config, Concurrency: 1}, func(context.Context, kq.Task) error {
		return cause
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(workerCtx) }()

	dlqConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics("dlq-test-dlq"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dlqConsumer.Close()

	client, err := kq.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	taskID, err := client.Enqueue(testCtx, kq.Task{Type: "test", Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}

	fetches := dlqConsumer.PollRecords(testCtx, 1)
	if err := fetches.Err(); err != nil {
		t.Fatal(err)
	}
	records := fetches.Records()
	if len(records) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(records))
	}
	record := records[0]
	if record.Topic != "dlq-test-dlq" || record.Partition != 0 {
		t.Fatalf("DLQ destination = %s/%d", record.Topic, record.Partition)
	}
	if string(record.Key) != taskID {
		t.Fatalf("DLQ key = %q, want %q", record.Key, taskID)
	}

	var envelope kqpb.TaskEnvelope
	if err := proto.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Id != taskID || envelope.Type != "test" || string(envelope.Payload) != "payload" {
		t.Fatalf("DLQ envelope = %v", &envelope)
	}
	if envelope.Retries != 0 || envelope.Retried != 0 {
		t.Fatalf("retry counts = %d/%d, want 0/0", envelope.Retried, envelope.Retries)
	}
	if envelope.LastError != cause.Error() {
		t.Fatalf("last error = %q, want %q", envelope.LastError, cause)
	}
	if envelope.LastErrorAt == nil {
		t.Fatal("last error time is nil")
	}
	if err := envelope.LastErrorAt.CheckValid(); err != nil {
		t.Fatalf("last error time is invalid: %v", err)
	}

	cancelWorker()
	if err := <-workerDone; err != nil {
		t.Fatalf("worker stopped: %v", err)
	}
}

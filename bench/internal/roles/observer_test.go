package roles

import (
	"context"
	"encoding/json"
	"testing"

	kqpb "github.com/raydatray/kq/internal/proto/kq"
	"github.com/raydatray/kq/bench/internal/events"
	"github.com/raydatray/kq/bench/internal/workload"
	"google.golang.org/protobuf/proto"
	"io"
)

func TestWorkloadIDFromDLQRecord(t *testing.T) {
	job := workload.Job{WorkloadID: 42}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	value, err := proto.Marshal(&kqpb.TaskEnvelope{
		Id:      "task-1",
		Type:    "bench",
		Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := workloadIDFromDLQRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("workload ID = %d, want 42", id)
	}
}

func TestWorkloadIDFromDLQRecordRejectsInvalid(t *testing.T) {
	if _, err := workloadIDFromDLQRecord([]byte("not-proto")); err == nil {
		t.Fatal("invalid proto = nil, want error")
	}
	value, err := proto.Marshal(&kqpb.TaskEnvelope{
		Id:      "task-1",
		Type:    "bench",
		Payload: []byte("not-json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workloadIDFromDLQRecord(value); err == nil {
		t.Fatal("invalid payload = nil, want error")
	}
}

func TestRunObserverRejectsEmptyConfig(t *testing.T) {
	config := testProducerConfig()
	config.KQ.Brokers = nil
	output := events.NewWriter(io.Discard)
	if err := RunObserver(context.Background(), config, output); err == nil {
		t.Fatal("empty brokers = nil, want error")
	}
	config = testProducerConfig()
	config.KQ.Queue = ""
	if err := RunObserver(context.Background(), config, output); err == nil {
		t.Fatal("empty queue = nil, want error")
	}
}

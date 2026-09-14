package kq

import (
	"testing"
	"time"

	kqpb "github.com/raydatray/kq/internal/proto/kq"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewTaskEnvelope(t *testing.T) {
	payload := []byte("hello")
	envelope, err := newTaskEnvelope(Task{Type: "email", Payload: payload}, 3)
	if err != nil {
		t.Fatal(err)
	}

	if envelope.Id == "" || envelope.Type != "email" || envelope.Retries != 3 {
		t.Fatalf("envelope = %+v", envelope)
	}
	if err := envelope.EnqueuedAt.CheckValid(); err != nil {
		t.Fatalf("enqueue time is invalid: %v", err)
	}

	payload[0] = 'x'
	if string(envelope.Payload) != "hello" {
		t.Fatalf("payload = %q, want %q", envelope.Payload, "hello")
	}
}

func TestNewTaskEnvelopeValidation(t *testing.T) {
	if _, err := newTaskEnvelope(Task{}, 0); err == nil {
		t.Fatal("empty task type accepted")
	}
	if _, err := newTaskEnvelope(Task{Type: "test"}, -1); err == nil {
		t.Fatal("negative retries accepted")
	}
}

func TestEnvelopeRoundTripPreservesMetadata(t *testing.T) {
	now := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
	want := &kqpb.TaskEnvelope{
		Id:          "task-id",
		Type:        "email",
		Payload:     []byte("hello"),
		EnqueuedAt:  timestamppb.New(now),
		Retries:     3,
		Retried:     2,
		RetryAfter:  durationpb.New(70 * time.Second),
		Timeout:     durationpb.New(time.Minute),
		Deadline:    timestamppb.New(now.Add(time.Hour)),
		LastError:   "temporary failure",
		LastErrorAt: timestamppb.New(now.Add(time.Minute)),
		Metadata:    map[string]string{"trace_id": "trace-id"},
	}

	value, err := encodeEnvelope(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeEnvelope(value)
	if err != nil {
		t.Fatal(err)
	}

	if !proto.Equal(got, want) {
		t.Fatalf("decoded envelope = %v, want %v", got, want)
	}
}

func TestDecodeEnvelopeRejectsInvalidEnqueueTime(t *testing.T) {
	envelope := &kqpb.TaskEnvelope{
		Id:         "task-id",
		Type:       "test",
		EnqueuedAt: &timestamppb.Timestamp{Seconds: 253402300800},
	}
	value, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := decodeEnvelope(value); err == nil {
		t.Fatal("invalid enqueue time accepted")
	}
}

func TestTaskFromEnvelopeCopiesPayload(t *testing.T) {
	envelope := &kqpb.TaskEnvelope{Type: "test", Payload: []byte("hello")}
	task := taskFromEnvelope(envelope)

	task.Payload[0] = 'x'
	if string(envelope.Payload) != "hello" {
		t.Fatalf("envelope payload = %q, want %q", envelope.Payload, "hello")
	}
}

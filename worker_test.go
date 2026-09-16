package kq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWorkerHandlesTask(t *testing.T) {
	writer := new(fakeWriter)
	value := encodeWorkerTestTask(t, Task{
		Type:    "send-email",
		Payload: []byte("hello"),
	}, 0)

	var received Task
	worker := &Worker{
		writer: writer,
		handler: func(_ context.Context, task Task) error {
			received = task
			return nil
		},
	}

	if err := worker.handle(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if received.Type != "send-email" {
		t.Fatalf("type = %q", received.Type)
	}
	if string(received.Payload) != "hello" {
		t.Fatalf("payload = %q", received.Payload)
	}
	if writer.record != nil {
		t.Fatalf("retry record = %v, want nil", writer.record)
	}
}

func TestWorkerSchedulesRetry(t *testing.T) {
	handlerErr := errors.New("failed")
	writer := new(fakeWriter)
	config := workerRetryConfig(t, 70*time.Second)
	value := encodeWorkerTestTask(t, Task{Type: "test", Payload: []byte("payload")}, 3)
	original, err := decodeEnvelope(value)
	if err != nil {
		t.Fatal(err)
	}
	var policyTaskID string
	config.RetryPolicy = newTestRetryPolicy(t, 3, func(_ int32, taskID string) time.Duration {
		policyTaskID = taskID
		return 70 * time.Second
	})

	worker := &Worker{
		writer: writer,
		config: config,
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}

	if err := worker.handle(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if policyTaskID != original.Id {
		t.Fatalf("retry policy task ID = %q, want %q", policyTaskID, original.Id)
	}
	if writer.record == nil {
		t.Fatal("retry record was not written")
	}
	if writer.record.Topic != "email-retry-0s" || writer.record.Partition != 2 {
		t.Fatalf("retry destination = %s/%d", writer.record.Topic, writer.record.Partition)
	}
	if string(writer.record.Key) != original.Id {
		t.Fatalf("retry key = %q, want %q", writer.record.Key, original.Id)
	}

	retried, err := decodeEnvelope(writer.record.Value)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Retried != 1 || retried.Retries != 3 {
		t.Fatalf("retry counts = %d/%d, want 1/3", retried.Retried, retried.Retries)
	}
	if retried.RetryAfter == nil || retried.RetryAfter.AsDuration() != 70*time.Second {
		t.Fatalf("retry after = %v, want 1m10s", retried.RetryAfter)
	}
	if retried.LastError != handlerErr.Error() {
		t.Fatalf("last error = %q, want %q", retried.LastError, handlerErr)
	}
	if retried.LastErrorAt == nil {
		t.Fatal("last error time is nil")
	}
	if err := retried.LastErrorAt.CheckValid(); err != nil {
		t.Fatalf("last error time is invalid: %v", err)
	}
}

func TestWorkerRetryWriteFailure(t *testing.T) {
	handlerErr := errors.New("handler failed")
	writeErr := errors.New("write failed")
	writer := &fakeWriter{err: writeErr}
	worker := &Worker{
		writer: writer,
		config: workerRetryConfig(t, time.Minute),
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test"}, 1)

	err := worker.handle(context.Background(), value)
	if !errors.Is(err, handlerErr) || !errors.Is(err, writeErr) {
		t.Fatalf("error = %v, want handler and write errors", err)
	}
}

func TestWorkerRetriesExhausted(t *testing.T) {
	handlerErr := errors.New("failed")
	writer := new(fakeWriter)
	worker := &Worker{
		writer: writer,
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test"}, 0)

	err := worker.handle(context.Background(), value)
	if !errors.Is(err, handlerErr) || !errors.Is(err, errRetriesExhausted) {
		t.Fatalf("error = %v, want handler and exhausted errors", err)
	}
	if writer.record != nil {
		t.Fatalf("retry record = %v, want nil", writer.record)
	}
}

func TestWorkerRejectsRetryOutsideGrid(t *testing.T) {
	handlerErr := errors.New("failed")
	writer := new(fakeWriter)
	worker := &Worker{
		writer: writer,
		config: workerRetryConfig(t, 3*time.Minute),
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test"}, 1)

	err := worker.handle(context.Background(), value)
	if !errors.Is(err, handlerErr) || !errors.Is(err, ErrRetryDelayOutOfRange) {
		t.Fatalf("error = %v, want handler and range errors", err)
	}
	if writer.record != nil {
		t.Fatalf("retry record = %v, want nil", writer.record)
	}
}

func TestWorkerRejectsInvalidRetryDelay(t *testing.T) {
	handlerErr := errors.New("failed")
	writer := new(fakeWriter)
	worker := &Worker{
		writer: writer,
		config: workerRetryConfig(t, -time.Second),
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test"}, 1)

	err := worker.handle(context.Background(), value)
	if !errors.Is(err, handlerErr) || !strings.Contains(err.Error(), "retry delay cannot be negative") {
		t.Fatalf("error = %v, want handler and invalid delay errors", err)
	}
	if writer.record != nil {
		t.Fatalf("retry record = %v, want nil", writer.record)
	}
}

func workerRetryConfig(t *testing.T, delay time.Duration) Config {
	t.Helper()

	grid, err := NewRetryGrid([]time.Duration{0, 2 * time.Minute}, 4)
	if err != nil {
		t.Fatal(err)
	}

	return Config{
		Queue:       "email",
		RetryPolicy: newTestRetryPolicy(t, 3, func(int32, string) time.Duration { return delay }),
		RetryGrid:   grid,
	}
}

func encodeWorkerTestTask(t *testing.T, task Task, retries int32) []byte {
	t.Helper()

	envelope, err := newTaskEnvelope(task, retries)
	if err != nil {
		t.Fatal(err)
	}
	value, err := encodeEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}

	return value
}

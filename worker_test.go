package kq

import (
	"context"
	"errors"
	"testing"
)

func TestWorkerHandlesTask(t *testing.T) {
	_, value, err := encodeTask(Task{
		Type:    "send-email",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}

	var received Task
	worker := &Worker{
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
}

func TestWorkerReturnsHandlerError(t *testing.T) {
	want := errors.New("failed")

	_, value, err := encodeTask(Task{Type: "test"})
	if err != nil {
		t.Fatal(err)
	}

	worker := &Worker{
		handler: func(context.Context, Task) error {
			return want
		},
	}

	if err := worker.handle(context.Background(), value); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

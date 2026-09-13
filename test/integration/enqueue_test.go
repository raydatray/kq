//go:build integration

package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/raydatray/kq"
)

func TestEnqueueAndWorkerPickup(t *testing.T) {
	broker := os.Getenv("KQ_TEST_BROKERS")
	if broker == "" {
		t.Skip("KQ_TEST_BROKERS is not set")
	}

	config := kq.Config{
		Brokers: []string{broker},
		Queue:   "test",
	}

	client, err := kq.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.Enqueue(context.Background(), kq.Task{
		Type:    "hello",
		Payload: []byte("world"),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	received := make(chan kq.Task, 1)
	worker, err := kq.NewWorker(config, func(_ context.Context, task kq.Task) error {
		received <- task
		cancel()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()

	select {
	case task := <-received:
		if task.Type != "hello" || string(task.Payload) != "world" {
			t.Fatalf("task = %#v", task)
		}
	case <-ctx.Done():
		t.Fatal("worker did not receive task")
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

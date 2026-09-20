//go:build integration

package integration_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raydatray/kq"
)

func TestTaskRetriesAndSucceeds(t *testing.T) {
	broker := os.Getenv("KQ_TEST_BROKERS")
	if broker == "" {
		t.Skip("KQ_TEST_BROKERS is not set")
	}

	grid, err := kq.NewRetryGrid([]time.Duration{0, 4 * time.Second}, 4)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := kq.NewRetryPolicy(1, func(int32, string) time.Duration { return time.Second })
	if err != nil {
		t.Fatal(err)
	}
	config := kq.Config{
		Brokers:     []string{broker},
		Queue:       "retry-test",
		RetryPolicy: policy,
		RetryGrid:   grid,
	}

	testCtx, cancelTest := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelTest()
	workerCtx, cancelWorker := context.WithCancel(testCtx)
	defer cancelWorker()
	moverCtx, cancelMover := context.WithCancel(testCtx)
	defer cancelMover()

	type delivery struct {
		task    kq.Task
		attempt int32
		at      time.Time
	}
	var attempts atomic.Int32
	deliveries := make(chan delivery, 2)
	handlerErr := errors.New("temporary failure")
	worker, err := kq.NewWorker(kq.WorkerConfig{Config: config, Concurrency: 1}, func(_ context.Context, task kq.Task) error {
		attempt := attempts.Add(1)
		deliveries <- delivery{task: task, attempt: attempt, at: time.Now()}
		if attempt == 1 {
			return handlerErr
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	mover, err := kq.NewRetryMover(config)
	if err != nil {
		t.Fatal(err)
	}
	defer mover.Close()

	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(workerCtx) }()
	moverDone := make(chan error, 1)
	go func() { moverDone <- mover.Run(moverCtx) }()

	client, err := kq.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Enqueue(testCtx, kq.Task{Type: "test", Payload: []byte("payload")}); err != nil {
		t.Fatal(err)
	}

	nextDelivery := func() delivery {
		t.Helper()
		select {
		case got := <-deliveries:
			return got
		case <-testCtx.Done():
			t.Fatal("timed out waiting for task delivery")
			return delivery{}
		}
	}
	first := nextDelivery()
	second := nextDelivery()

	for _, got := range []delivery{first, second} {
		if got.task.Type != "test" || string(got.task.Payload) != "payload" {
			t.Fatalf("delivery = %#v", got)
		}
	}
	if first.attempt != 1 || second.attempt != 2 {
		t.Fatalf("attempts = %d, %d; want 1, 2", first.attempt, second.attempt)
	}
	if elapsed := second.at.Sub(first.at); elapsed < time.Second {
		t.Fatalf("retry delivered after %s, want at least 1s", elapsed)
	}

	cancelWorker()
	if err := <-workerDone; err != nil {
		t.Fatalf("worker stopped: %v", err)
	}
	cancelMover()
	if err := <-moverDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("retry mover stopped: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

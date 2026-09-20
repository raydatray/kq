//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raydatray/kq"
)

func TestWorkerConcurrency(t *testing.T) {
	broker := os.Getenv("KQ_TEST_BROKERS")
	if broker == "" {
		t.Skip("KQ_TEST_BROKERS is not set")
	}

	const (
		concurrency = 4
		taskCount   = 9
	)
	config := kq.Config{
		Brokers: []string{broker},
		Queue:   "concurrency-test",
	}
	testCtx, cancelTest := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelTest()

	client, err := kq.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	for i := range taskCount {
		if _, err := client.Enqueue(testCtx, kq.Task{
			Type:    "concurrency",
			Payload: []byte(fmt.Sprintf("task-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	client.Close()

	started := make(chan string, taskCount)
	completed := make(chan string, taskCount)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	worker, err := kq.NewWorker(kq.WorkerConfig{
		Config:      config,
		Concurrency: concurrency,
	}, func(ctx context.Context, task kq.Task) error {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}

		id := string(task.Payload)
		started <- id
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			completed <- id
			return nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	workerCtx, cancelWorker := context.WithCancel(testCtx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(workerCtx) }()

	firstGeneration := make(map[string]bool, concurrency)
	for range concurrency {
		select {
		case id := <-started:
			if firstGeneration[id] {
				t.Fatalf("task %q started twice", id)
			}
			firstGeneration[id] = true
		case <-testCtx.Done():
			t.Fatal("worker did not fill its concurrency pool")
		}
	}
	if got := maximum.Load(); got != concurrency {
		t.Fatalf("maximum concurrency = %d, want %d", got, concurrency)
	}
	close(release)

	seen := make(map[string]bool, taskCount)
	for len(seen) < taskCount {
		select {
		case id := <-completed:
			if seen[id] {
				t.Fatalf("task %q completed twice", id)
			}
			seen[id] = true
		case <-testCtx.Done():
			t.Fatalf("completed %d/%d tasks", len(seen), taskCount)
		}
	}

	cancelWorker()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got > concurrency {
		t.Fatalf("maximum concurrency = %d, want at most %d", got, concurrency)
	}
}

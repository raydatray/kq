package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriterEmitsNDJSON(t *testing.T) {
	var buf bytes.Buffer
	writer := NewWriter(&buf)

	started := time.Now().Add(-time.Millisecond)
	if err := writer.Write(ProcessReady(RoleProducer, 0)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(EnqueueFinished(42, 0, started, nil)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(EnqueueFinished(43, 0, started, errors.New("boom"))); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want 3", len(lines))
	}
	var events []Event
	for _, line := range lines {
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if events[0].Type != TypeProcessReady || events[0].Role != RoleProducer {
		t.Fatalf("first event = %#v", events[0])
	}
	if events[1].Type != TypeEnqueueFinished || events[1].Result != ResultSuccess || events[1].WorkloadID != 42 {
		t.Fatalf("second event = %#v", events[1])
	}
	if events[1].DurationNS <= 0 {
		t.Fatalf("duration = %d, want positive", events[1].DurationNS)
	}
	if events[2].Result != ResultError || events[2].Error != "boom" {
		t.Fatalf("third event = %#v", events[2])
	}
}

func TestWriterConcurrent(t *testing.T) {
	var buf bytes.Buffer
	writer := NewWriter(&buf)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(process int) {
			defer wg.Done()
			for id := 0; id < 50; id++ {
				if err := writer.Write(HandlerStarted(uint64(id), process)); err != nil {
					t.Errorf("write event: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 500 {
		t.Fatalf("lines = %d, want 500", len(lines))
	}
	for _, line := range lines {
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("invalid JSON line: %v", err)
		}
	}
}

func TestWriterDoesNotWaitForOutput(t *testing.T) {
	output := &blockingOutput{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	writer := NewWriter(output)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writer.Write(HandlerStarted(1, 0))
	}()

	select {
	case <-output.started:
	case <-time.After(time.Second):
		t.Fatal("output write did not start")
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("event write waited for output")
	}

	close(output.release)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterReportsOutputErrorOnClose(t *testing.T) {
	want := errors.New("write failed")
	writer := NewWriter(errorOutput{err: want})
	if err := writer.Write(HandlerStarted(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); !errors.Is(err, want) {
		t.Fatalf("Close() error = %v, want %v", err, want)
	}
}

func TestWriterRejectsWriteAfterClose(t *testing.T) {
	writer := NewWriter(new(bytes.Buffer))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(HandlerStarted(1, 0)); !errors.Is(err, errWriterClosed) {
		t.Fatalf("Write() error = %v, want %v", err, errWriterClosed)
	}
}

type blockingOutput struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingOutput) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

type errorOutput struct {
	err error
}

func (w errorOutput) Write([]byte) (int, error) {
	return 0, w.err
}

func TestEventHelpers(t *testing.T) {
	started := time.Now().Add(-2 * time.Millisecond)

	finished := HandlerFinished(7, 1, started, nil)
	if finished.Type != TypeHandlerFinished || finished.Result != ResultSuccess || finished.WorkloadID != 7 {
		t.Fatalf("handler finished = %#v", finished)
	}
	if finished.DurationNS <= 0 {
		t.Fatalf("handler duration = %d", finished.DurationNS)
	}

	failed := HandlerFinished(8, 1, started, errors.New("fail"))
	if failed.Result != ResultError || failed.Error != "fail" {
		t.Fatalf("failed handler = %#v", failed)
	}

	dead := DeadLettered(9, time.Now())
	if dead.Type != TypeDeadLettered || dead.Role != RoleObserver || dead.WorkloadID != 9 {
		t.Fatalf("dead lettered = %#v", dead)
	}

	stopped := ProcessStopped(RoleWorker, 2, nil)
	if stopped.Type != TypeProcessStopped || stopped.Result != ResultSuccess {
		t.Fatalf("stopped = %#v", stopped)
	}
	stoppedErr := ProcessStopped(RoleWorker, 2, errors.New("x"))
	if stoppedErr.Result != ResultError {
		t.Fatalf("stopped err = %#v", stoppedErr)
	}
}

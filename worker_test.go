package kq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeShareGroupConsumer struct {
	mu           sync.Mutex
	results      []sharePollResult
	pollLimits   []int
	acks         map[*kgo.Record][]kgo.AckStatus
	ackOrder     []*kgo.Record
	acked        chan *kgo.Record
	flushes      int
	flushErr     error
	flushStarted chan struct{}
	flushRelease chan struct{}
}

func (c *fakeShareGroupConsumer) Poll(ctx context.Context, limit int) sharePollResult {
	c.mu.Lock()
	c.pollLimits = append(c.pollLimits, limit)
	if len(c.results) > 0 {
		result := c.results[0]
		c.results = c.results[1:]
		c.mu.Unlock()
		return result
	}
	c.mu.Unlock()

	<-ctx.Done()
	return sharePollResult{err: ctx.Err()}
}

func (c *fakeShareGroupConsumer) Ack(record *kgo.Record, status kgo.AckStatus) {
	c.mu.Lock()
	if c.acks == nil {
		c.acks = make(map[*kgo.Record][]kgo.AckStatus)
	}
	c.acks[record] = append(c.acks[record], status)
	c.ackOrder = append(c.ackOrder, record)
	acked := c.acked
	c.mu.Unlock()
	if acked != nil {
		acked <- record
	}
}

func (c *fakeShareGroupConsumer) FlushAcks(context.Context) error {
	c.mu.Lock()
	c.flushes++
	err := c.flushErr
	started := c.flushStarted
	release := c.flushRelease
	c.mu.Unlock()
	if started != nil {
		started <- struct{}{}
	}
	if release != nil {
		<-release
	}
	return err
}

func (*fakeShareGroupConsumer) Close() {}

func (c *fakeShareGroupConsumer) pollCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pollLimits)
}

func (c *fakeShareGroupConsumer) statuses(record *kgo.Record) []kgo.AckStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]kgo.AckStatus(nil), c.acks[record]...)
}

func (c *fakeShareGroupConsumer) flushCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushes
}

func TestWorkerProcessesBoundedPollGeneration(t *testing.T) {
	records := []*kgo.Record{
		{Value: encodeWorkerTestTask(t, Task{Type: "first"}, 0)},
		{Value: encodeWorkerTestTask(t, Task{Type: "second"}, 0)},
	}
	consumer := &fakeShareGroupConsumer{results: []sharePollResult{
		{records: records},
		{closed: true},
	}}
	handled := make(chan string, len(records))
	worker := &Worker{
		consumer: consumer,
		producer: new(fakeProducer),
		config:   WorkerConfig{Concurrency: 3},
		handler: func(_ context.Context, task Task) error {
			handled <- task.Type
			return nil
		},
	}

	if err := worker.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for range records {
		seen[<-handled] = true
	}
	if !seen["first"] || !seen["second"] {
		t.Fatalf("handled = %v, want first and second", seen)
	}
	if len(consumer.pollLimits) != 2 || consumer.pollLimits[0] != 3 || consumer.pollLimits[1] != 3 {
		t.Fatalf("poll limits = %v, want [3 3]", consumer.pollLimits)
	}
	for _, record := range records {
		statuses := consumer.acks[record]
		if len(statuses) != 1 || statuses[0] != kgo.AckAccept {
			t.Fatalf("ack statuses = %v, want [accept]", statuses)
		}
	}
	if consumer.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", consumer.flushes)
	}
}

func TestWorkerExecutesGenerationConcurrently(t *testing.T) {
	const concurrency = 4
	records := make([]*kgo.Record, 0, concurrency)
	for i := range concurrency {
		records = append(records, &kgo.Record{
			Value: encodeWorkerTestTask(t, Task{Type: fmt.Sprintf("task-%d", i)}, 0),
		})
	}
	consumer := &fakeShareGroupConsumer{results: []sharePollResult{
		{records: records},
		{closed: true},
	}}
	started := make(chan struct{}, concurrency)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	worker := &Worker{
		consumer: consumer,
		producer: new(fakeProducer),
		config:   WorkerConfig{Concurrency: concurrency},
		handler: func(context.Context, Task) error {
			current := active.Add(1)
			for {
				prior := maximum.Load()
				if current <= prior || maximum.CompareAndSwap(prior, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(context.Background()) }()

	for range concurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("generation did not execute concurrently")
		}
	}
	if got := maximum.Load(); got != concurrency {
		t.Fatalf("maximum concurrency = %d, want %d", got, concurrency)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerFlushesGenerationBeforeNextPoll(t *testing.T) {
	records := []*kgo.Record{
		{Value: encodeWorkerTestTask(t, Task{Type: "first"}, 0)},
		{Value: encodeWorkerTestTask(t, Task{Type: "second"}, 0)},
	}
	consumer := &fakeShareGroupConsumer{
		results: []sharePollResult{
			{records: records[:1]},
			{records: records[1:]},
			{closed: true},
		},
		flushStarted: make(chan struct{}, 2),
		flushRelease: make(chan struct{}),
	}
	worker := &Worker{
		consumer: consumer,
		producer: new(fakeProducer),
		config:   WorkerConfig{Concurrency: 1},
		handler:  func(context.Context, Task) error { return nil },
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(context.Background()) }()

	<-consumer.flushStarted
	if got := consumer.pollCount(); got != 1 {
		t.Fatalf("polls during first flush = %d, want 1", got)
	}
	consumer.flushRelease <- struct{}{}
	<-consumer.flushStarted
	if got := consumer.pollCount(); got != 2 {
		t.Fatalf("polls during second flush = %d, want 2", got)
	}
	consumer.flushRelease <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := consumer.pollCount(); got != 3 {
		t.Fatalf("total polls = %d, want 3", got)
	}
}

func TestWorkerAcknowledgesTasksAsTheyFinish(t *testing.T) {
	slow := &kgo.Record{Value: encodeWorkerTestTask(t, Task{Type: "slow"}, 0)}
	fast := &kgo.Record{Value: encodeWorkerTestTask(t, Task{Type: "fast"}, 0)}
	consumer := &fakeShareGroupConsumer{
		results: []sharePollResult{{records: []*kgo.Record{slow, fast}}, {closed: true}},
		acked:   make(chan *kgo.Record, 2),
	}
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	worker := &Worker{
		consumer: consumer,
		producer: new(fakeProducer),
		config:   WorkerConfig{Concurrency: 2},
		handler: func(_ context.Context, task Task) error {
			if task.Type == "slow" {
				close(slowStarted)
				<-releaseSlow
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(context.Background()) }()

	<-slowStarted
	if first := <-consumer.acked; first != fast {
		t.Fatalf("first acknowledged record = %p, want fast record %p", first, fast)
	}
	close(releaseSlow)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerJoinsConcurrentTaskAndFlushErrors(t *testing.T) {
	first := errors.New("first handler failed")
	second := errors.New("second handler failed")
	writeErr := errors.New("retry write failed")
	flushErr := errors.New("ack flush failed")
	records := []*kgo.Record{
		{Value: encodeWorkerTestTask(t, Task{Type: "first"}, 1)},
		{Value: encodeWorkerTestTask(t, Task{Type: "second"}, 1)},
	}
	consumer := &fakeShareGroupConsumer{
		results:  []sharePollResult{{records: records}},
		flushErr: flushErr,
	}
	config := workerRetryConfig(t, time.Minute)
	config.Concurrency = 2
	worker := &Worker{
		consumer: consumer,
		producer: &fakeProducer{err: writeErr},
		config:   config,
		handler: func(_ context.Context, task Task) error {
			if task.Type == "first" {
				return first
			}
			return second
		},
	}

	err := worker.Run(context.Background())
	for _, want := range []error{first, second, writeErr, flushErr} {
		if !errors.Is(err, want) {
			t.Fatalf("error = %v, want cause %v", err, want)
		}
	}
	for _, record := range records {
		statuses := consumer.statuses(record)
		if len(statuses) != 1 || statuses[0] != kgo.AckRelease {
			t.Fatalf("statuses = %v, want [release]", statuses)
		}
	}
}

func TestWorkerCancellationDrainsPool(t *testing.T) {
	const concurrency = 4
	records := make([]*kgo.Record, 0, concurrency)
	for i := range concurrency {
		records = append(records, &kgo.Record{
			Value: encodeWorkerTestTask(t, Task{Type: fmt.Sprintf("task-%d", i)}, 0),
		})
	}
	consumer := &fakeShareGroupConsumer{results: []sharePollResult{{records: records}}}
	started := make(chan struct{}, concurrency)
	worker := &Worker{
		consumer: consumer,
		producer: new(fakeProducer),
		config:   WorkerConfig{Concurrency: concurrency},
		handler: func(ctx context.Context, _ Task) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()

	for range concurrency {
		<-started
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not drain canceled pool")
	}
	for _, record := range records {
		statuses := consumer.statuses(record)
		if len(statuses) != 1 || statuses[0] != kgo.AckRelease {
			t.Fatalf("statuses = %v, want [release]", statuses)
		}
	}
	if got := consumer.flushCount(); got != 1 {
		t.Fatalf("flushes = %d, want 1", got)
	}
}

func TestWorkerHandlesTask(t *testing.T) {
	producer := new(fakeProducer)
	value := encodeWorkerTestTask(t, Task{
		Type:    "send-email",
		Payload: []byte("hello"),
	}, 0)

	var received Task
	worker := &Worker{
		producer: producer,
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
	if producer.record != nil {
		t.Fatalf("retry record = %v, want nil", producer.record)
	}
}

func TestWorkerSchedulesRetry(t *testing.T) {
	handlerErr := errors.New("failed")
	producer := new(fakeProducer)
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
		producer: producer,
		config:   config,
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
	if producer.record == nil {
		t.Fatal("retry record was not written")
	}
	if producer.record.Topic != "email-retry-0s" || producer.record.Partition != 2 {
		t.Fatalf("retry destination = %s/%d", producer.record.Topic, producer.record.Partition)
	}
	if string(producer.record.Key) != original.Id {
		t.Fatalf("retry key = %q, want %q", producer.record.Key, original.Id)
	}

	retried, err := decodeEnvelope(producer.record.Value)
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
	producer := &fakeProducer{err: writeErr}
	worker := &Worker{
		producer: producer,
		config:   workerRetryConfig(t, time.Minute),
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

func TestWorkerDeadLettersExhaustedTask(t *testing.T) {
	handlerErr := errors.New("failed")
	producer := new(fakeProducer)
	worker := &Worker{
		producer: producer,
		config:   WorkerConfig{Config: Config{Queue: "email"}},
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test", Payload: []byte("payload")}, 0)
	original, err := decodeEnvelope(value)
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.handle(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if producer.record == nil {
		t.Fatal("dead letter record was not written")
	}
	if producer.record.Topic != "email-dlq" || producer.record.Partition != 0 {
		t.Fatalf("dead letter destination = %s/%d", producer.record.Topic, producer.record.Partition)
	}
	if string(producer.record.Key) != original.Id {
		t.Fatalf("dead letter key = %q, want %q", producer.record.Key, original.Id)
	}

	letter, err := decodeEnvelope(producer.record.Value)
	if err != nil {
		t.Fatal(err)
	}
	if letter.Retried != 0 || letter.Retries != 0 {
		t.Fatalf("retry counts = %d/%d, want 0/0", letter.Retried, letter.Retries)
	}
	if letter.Type != "test" || string(letter.Payload) != "payload" {
		t.Fatalf("dead letter envelope = %v", letter)
	}
	if letter.LastError != handlerErr.Error() {
		t.Fatalf("last error = %q, want %q", letter.LastError, handlerErr)
	}
	if letter.LastErrorAt == nil {
		t.Fatal("last error time is nil")
	}
	if err := letter.LastErrorAt.CheckValid(); err != nil {
		t.Fatalf("last error time is invalid: %v", err)
	}
}

func TestWorkerDeadLetterWriteFailure(t *testing.T) {
	handlerErr := errors.New("handler failed")
	writeErr := errors.New("DLQ write failed")
	producer := &fakeProducer{err: writeErr}
	worker := &Worker{
		producer: producer,
		config:   WorkerConfig{Config: Config{Queue: "email"}},
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test"}, 0)

	err := worker.handle(context.Background(), value)
	if !errors.Is(err, handlerErr) || !errors.Is(err, writeErr) {
		t.Fatalf("error = %v, want handler and DLQ write errors", err)
	}
	if producer.record == nil || producer.record.Topic != "email-dlq" {
		t.Fatalf("dead letter record = %v", producer.record)
	}
}

func TestWorkerRejectsRetryOutsideGrid(t *testing.T) {
	handlerErr := errors.New("failed")
	producer := new(fakeProducer)
	worker := &Worker{
		producer: producer,
		config:   workerRetryConfig(t, 3*time.Minute),
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test"}, 1)

	err := worker.handle(context.Background(), value)
	if !errors.Is(err, handlerErr) || !errors.Is(err, ErrRetryDelayOutOfRange) {
		t.Fatalf("error = %v, want handler and range errors", err)
	}
	if producer.record != nil {
		t.Fatalf("retry record = %v, want nil", producer.record)
	}
}

func TestWorkerRejectsInvalidRetryDelay(t *testing.T) {
	handlerErr := errors.New("failed")
	producer := new(fakeProducer)
	worker := &Worker{
		producer: producer,
		config:   workerRetryConfig(t, -time.Second),
		handler: func(context.Context, Task) error {
			return handlerErr
		},
	}
	value := encodeWorkerTestTask(t, Task{Type: "test"}, 1)

	err := worker.handle(context.Background(), value)
	if !errors.Is(err, handlerErr) || !strings.Contains(err.Error(), "retry delay cannot be negative") {
		t.Fatalf("error = %v, want handler and invalid delay errors", err)
	}
	if producer.record != nil {
		t.Fatalf("retry record = %v, want nil", producer.record)
	}
}

func workerRetryConfig(t *testing.T, delay time.Duration) WorkerConfig {
	t.Helper()

	grid, err := NewRetryGrid([]time.Duration{0, 2 * time.Minute}, 4)
	if err != nil {
		t.Fatal(err)
	}

	return WorkerConfig{
		Config: Config{
			Queue:       "email",
			RetryPolicy: newTestRetryPolicy(t, 3, func(int32, string) time.Duration { return delay }),
			RetryGrid:   grid,
		},
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

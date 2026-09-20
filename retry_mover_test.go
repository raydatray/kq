package kq

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/durationpb"
)

type fakeRetryConsumer struct {
	results   []retryPollResult
	paused    []topicPartition
	resumed   []topicPartition
	committed []*kgo.Record
	commitErr error
	closed    bool
	events    *[]string
}

func (c *fakeRetryConsumer) Poll(ctx context.Context) retryPollResult {
	if len(c.results) > 0 {
		result := c.results[0]
		c.results = c.results[1:]
		return result
	}

	<-ctx.Done()
	return retryPollResult{err: ctx.Err()}
}

func (c *fakeRetryConsumer) Pause(partition topicPartition) {
	c.paused = append(c.paused, partition)
}

func (c *fakeRetryConsumer) Resume(partition topicPartition) {
	c.resumed = append(c.resumed, partition)
}

func (c *fakeRetryConsumer) Commit(_ context.Context, record *kgo.Record) error {
	c.committed = append(c.committed, record)
	if c.events != nil {
		*c.events = append(*c.events, "commit")
	}
	return c.commitErr
}

func (c *fakeRetryConsumer) Close() {
	c.closed = true
}

type fakeRetryProducer struct {
	records []*kgo.Record
	err     error
	closed  bool
	events  *[]string
}

func (p *fakeRetryProducer) Produce(_ context.Context, record *kgo.Record) error {
	p.records = append(p.records, record)
	if p.events != nil {
		*p.events = append(*p.events, "produce")
	}
	return p.err
}

func (p *fakeRetryProducer) Close() {
	p.closed = true
}

func TestRetryMoverMovesDueRecord(t *testing.T) {
	var events []string
	consumer := &fakeRetryConsumer{events: &events}
	producer := &fakeRetryProducer{events: &events}
	mover := newTestRetryMover(consumer, producer)
	record := newRetryMoverRecord(t, time.Minute, time.Now().Add(-2*time.Minute))

	if err := mover.handleRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(events, []string{"produce", "commit"}) {
		t.Fatalf("events = %v, want [produce commit]", events)
	}
	if len(producer.records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(producer.records))
	}
	produced := producer.records[0]
	if produced.Topic != "email-ready" || !bytes.Equal(produced.Key, record.Key) || !bytes.Equal(produced.Value, record.Value) {
		t.Fatalf("produced record = %+v", produced)
	}
	if len(consumer.committed) != 1 || consumer.committed[0] != record {
		t.Fatalf("committed records = %v, want source record", consumer.committed)
	}
}

func TestRetryMoverHoldsFutureRecord(t *testing.T) {
	consumer := new(fakeRetryConsumer)
	producer := new(fakeRetryProducer)
	mover := newTestRetryMover(consumer, producer)
	record := newRetryMoverRecord(t, time.Minute, time.Now())
	partition := topicPartition{topic: record.Topic, partition: record.Partition}

	if err := mover.handleRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(consumer.paused, []topicPartition{partition}) {
		t.Fatalf("paused partitions = %v, want %v", consumer.paused, partition)
	}
	if len(mover.held[partition]) != 1 || mover.held[partition][0].record != record {
		t.Fatalf("held records = %v, want source record", mover.held[partition])
	}
	if len(producer.records) != 0 || len(consumer.committed) != 0 {
		t.Fatal("future record was moved")
	}
}

func TestRetryMoverPreservesPartitionOrder(t *testing.T) {
	consumer := new(fakeRetryConsumer)
	producer := new(fakeRetryProducer)
	mover := newTestRetryMover(consumer, producer)
	first := newRetryMoverRecord(t, time.Minute, time.Now())
	second := newRetryMoverRecord(t, time.Minute, time.Now().Add(-2*time.Minute))
	second.Offset = first.Offset + 1

	if err := mover.handleRecord(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := mover.handleRecord(context.Background(), second); err != nil {
		t.Fatal(err)
	}

	partition := topicPartition{topic: first.Topic, partition: first.Partition}
	if len(mover.held[partition]) != 2 {
		t.Fatalf("held records = %d, want 2", len(mover.held[partition]))
	}
	if len(consumer.paused) != 1 {
		t.Fatalf("pause calls = %d, want 1", len(consumer.paused))
	}
	if len(producer.records) != 0 || len(consumer.committed) != 0 {
		t.Fatal("record moved past an earlier held offset")
	}
}

func TestRetryMoverMovesHeldRecordAndResumesPartition(t *testing.T) {
	consumer := new(fakeRetryConsumer)
	producer := new(fakeRetryProducer)
	mover := newTestRetryMover(consumer, producer)
	record := newRetryMoverRecord(t, time.Minute, time.Now().Add(-2*time.Minute))
	partition := topicPartition{topic: record.Topic, partition: record.Partition}
	mover.held[partition] = []heldRetry{{record: record, dueAt: time.Now().Add(-time.Second)}}

	moved, err := mover.moveDueRecord(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !moved {
		t.Fatal("moveDueRecord() moved = false")
	}
	if len(mover.held) != 0 {
		t.Fatalf("held partitions = %d, want 0", len(mover.held))
	}
	if !reflect.DeepEqual(consumer.resumed, []topicPartition{partition}) {
		t.Fatalf("resumed partitions = %v, want %v", consumer.resumed, partition)
	}
}

func TestRetryMoverDoesNotResumePartitionWithMoreHeldRecords(t *testing.T) {
	consumer := new(fakeRetryConsumer)
	producer := new(fakeRetryProducer)
	mover := newTestRetryMover(consumer, producer)
	first := newRetryMoverRecord(t, time.Minute, time.Now().Add(-2*time.Minute))
	second := newRetryMoverRecord(t, time.Minute, time.Now())
	partition := topicPartition{topic: first.Topic, partition: first.Partition}
	mover.held[partition] = []heldRetry{
		{record: first, dueAt: time.Now().Add(-time.Second)},
		{record: second, dueAt: time.Now().Add(time.Minute)},
	}

	moved, err := mover.moveDueRecord(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !moved || len(mover.held[partition]) != 1 {
		t.Fatalf("moved = %t, held records = %d", moved, len(mover.held[partition]))
	}
	if len(consumer.resumed) != 0 {
		t.Fatalf("resumed partitions = %v, want none", consumer.resumed)
	}
}

func TestRetryMoverProduceFailureDoesNotCommit(t *testing.T) {
	produceErr := errors.New("produce failed")
	consumer := new(fakeRetryConsumer)
	producer := &fakeRetryProducer{err: produceErr}
	mover := newTestRetryMover(consumer, producer)
	record := newRetryMoverRecord(t, time.Minute, time.Now().Add(-2*time.Minute))

	err := mover.move(context.Background(), record)
	if !errors.Is(err, produceErr) {
		t.Fatalf("error = %v, want produce error", err)
	}
	if len(consumer.committed) != 0 {
		t.Fatal("record committed after produce failure")
	}
}

func TestRetryMoverCommitFailure(t *testing.T) {
	commitErr := errors.New("commit failed")
	consumer := &fakeRetryConsumer{commitErr: commitErr}
	producer := new(fakeRetryProducer)
	mover := newTestRetryMover(consumer, producer)
	record := newRetryMoverRecord(t, time.Minute, time.Now().Add(-2*time.Minute))

	err := mover.move(context.Background(), record)
	if !errors.Is(err, commitErr) {
		t.Fatalf("error = %v, want commit error", err)
	}
	if len(producer.records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(producer.records))
	}
}

func TestRetryMoverRejectsInvalidRetryDelay(t *testing.T) {
	tests := []struct {
		name       string
		retryAfter *durationpb.Duration
		want       string
	}{
		{name: "missing", want: "invalid retry delay"},
		{name: "negative", retryAfter: durationpb.New(-time.Second), want: "retry delay cannot be negative"},
		{name: "malformed", retryAfter: &durationpb.Duration{Seconds: 1, Nanos: -1}, want: "invalid retry delay"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := new(fakeRetryConsumer)
			producer := new(fakeRetryProducer)
			mover := newTestRetryMover(consumer, producer)
			record := newRetryMoverRecordWithDelay(t, test.retryAfter, time.Now())

			err := mover.handleRecord(context.Background(), record)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if len(consumer.paused) != 0 || len(consumer.committed) != 0 || len(producer.records) != 0 {
				t.Fatal("invalid record caused side effects")
			}
		})
	}
}

func TestRetryMoverNextWakeUsesPartitionFronts(t *testing.T) {
	mover := newTestRetryMover(new(fakeRetryConsumer), new(fakeRetryProducer))
	first := time.Now().Add(time.Minute)
	earlierBehindFront := first.Add(-30 * time.Second)
	earliestFront := first.Add(-10 * time.Second)
	mover.held = map[topicPartition][]heldRetry{
		{topic: "first", partition: 0}: {
			{dueAt: first},
			{dueAt: earlierBehindFront},
		},
		{topic: "second", partition: 0}: {
			{dueAt: earliestFront},
		},
	}

	if got := mover.nextWake(); !got.Equal(earliestFront) {
		t.Fatalf("next wake = %s, want %s", got, earliestFront)
	}
}

func TestRetryMoverRunProcessesRecordBeforePollError(t *testing.T) {
	pollErr := errors.New("poll failed")
	record := newRetryMoverRecord(t, time.Minute, time.Now().Add(-2*time.Minute))
	consumer := &fakeRetryConsumer{results: []retryPollResult{{record: record, err: pollErr}}}
	producer := new(fakeRetryProducer)
	mover := newTestRetryMover(consumer, producer)

	err := mover.Run(context.Background())
	if !errors.Is(err, pollErr) {
		t.Fatalf("error = %v, want poll error", err)
	}
	if len(producer.records) != 1 || len(consumer.committed) != 1 {
		t.Fatal("record was not moved before returning poll error")
	}
}

func TestRetryMoverRunStopsNormally(t *testing.T) {
	tests := []struct {
		name     string
		result   retryPollResult
		canceled bool
	}{
		{name: "consumer closed", result: retryPollResult{closed: true}},
		{name: "context canceled", canceled: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := &fakeRetryConsumer{results: []retryPollResult{test.result}}
			mover := newTestRetryMover(consumer, new(fakeRetryProducer))
			ctx, cancel := context.WithCancel(context.Background())
			if test.canceled {
				cancel()
			} else {
				defer cancel()
			}

			if err := mover.Run(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetryMoverClose(t *testing.T) {
	consumer := new(fakeRetryConsumer)
	producer := new(fakeRetryProducer)
	mover := newTestRetryMover(consumer, producer)

	mover.Close()

	if !consumer.closed || !producer.closed {
		t.Fatalf("closed consumer/producer = %t/%t, want true/true", consumer.closed, producer.closed)
	}
}

func newTestRetryMover(consumer retryConsumer, producer recordProducer) *RetryMover {
	return &RetryMover{
		config:   Config{Queue: "email"},
		consumer: consumer,
		producer: producer,
		held:     make(map[topicPartition][]heldRetry),
	}
}

func newRetryMoverRecord(t *testing.T, retryAfter time.Duration, timestamp time.Time) *kgo.Record {
	t.Helper()
	return newRetryMoverRecordWithDelay(t, durationpb.New(retryAfter), timestamp)
}

func newRetryMoverRecordWithDelay(t *testing.T, retryAfter *durationpb.Duration, timestamp time.Time) *kgo.Record {
	t.Helper()

	envelope, err := newTaskEnvelope(Task{Type: "test", Payload: []byte("payload")}, 3)
	if err != nil {
		t.Fatal(err)
	}
	envelope.RetryAfter = retryAfter
	value, err := encodeEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}

	return &kgo.Record{
		Topic:     "email-retry-0s",
		Partition: 0,
		Offset:    10,
		Key:       []byte(envelope.Id),
		Value:     value,
		Timestamp: timestamp,
	}
}

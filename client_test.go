package kq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProducer struct {
	mu      sync.Mutex
	record  *kgo.Record
	records []*kgo.Record
	err     error
}

func (p *fakeProducer) Produce(_ context.Context, record *kgo.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.record = record
	p.records = append(p.records, record)
	return p.err
}

func (*fakeProducer) Close() {}

func TestClientEnqueue(t *testing.T) {
	producer := new(fakeProducer)
	client := &Client{
		config: Config{
			Queue:       "email",
			RetryPolicy: newTestRetryPolicy(t, 3, func(int32, string) time.Duration { return time.Minute }),
		},
		producer: producer,
	}

	id, err := client.Enqueue(context.Background(), Task{
		Type:    "send-email",
		Payload: []byte(`{"to":"ray@example.com"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	if producer.record.Topic != "email-ready" {
		t.Fatalf("topic = %q", producer.record.Topic)
	}
	if string(producer.record.Key) != id {
		t.Fatalf("key = %q, want %q", producer.record.Key, id)
	}

	envelope, err := decodeEnvelope(producer.record.Value)
	if err != nil {
		t.Fatal(err)
	}
	task := taskFromEnvelope(envelope)
	if envelope.Id != id || envelope.Retries != 3 || task.Type != "send-email" {
		t.Fatalf("decoded task = %#v", task)
	}
}

package kq

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeWriter struct {
	record *kgo.Record
	err    error
}

func (w *fakeWriter) Write(_ context.Context, record *kgo.Record) error {
	w.record = record
	return w.err
}

func (*fakeWriter) Close() {}

func TestClientEnqueue(t *testing.T) {
	writer := new(fakeWriter)
	client := &Client{
		config: Config{Queue: "email"},
		writer: writer,
	}

	id, err := client.Enqueue(context.Background(), Task{
		Type:    "send-email",
		Payload: []byte(`{"to":"ray@example.com"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	if writer.record.Topic != "email-ready" {
		t.Fatalf("topic = %q", writer.record.Topic)
	}
	if string(writer.record.Key) != id {
		t.Fatalf("key = %q, want %q", writer.record.Key, id)
	}

	gotID, task, err := decodeTask(writer.record.Value)
	if err != nil {
		t.Fatal(err)
	}
	if gotID != id || task.Type != "send-email" {
		t.Fatalf("decoded task = %#v", task)
	}
}

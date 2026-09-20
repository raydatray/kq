package kq

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaShareGroupConsumerTracksAckErrors(t *testing.T) {
	first := errors.New("first ack failed")
	second := errors.New("second ack failed")
	consumer := new(kafkaShareGroupConsumer)

	consumer.recordAckResult(nil, kgo.ShareAckResults{{Err: first}})
	consumer.recordAckResult(nil, kgo.ShareAckResults{{Err: second}})

	err := consumer.takeAckError()
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("error = %v, want both ack errors", err)
	}
	if err := consumer.takeAckError(); err != nil {
		t.Fatalf("second take = %v, want nil", err)
	}
}

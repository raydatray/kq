package kq

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type recordProducer interface {
	Produce(context.Context, *kgo.Record) error
	Close()
}

type kafkaProducer struct {
	client *kgo.Client
}

func newKafkaProducer(config Config, options ...kgo.Opt) (*kafkaProducer, error) {
	options = append(options, kgo.RequiredAcks(kgo.AllISRAcks()))
	kafkaOptions, err := config.kafkaOptions(options...)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		return nil, fmt.Errorf("kq: create Kafka producer: %w", err)
	}

	return &kafkaProducer{client: client}, nil
}

func (p *kafkaProducer) Produce(ctx context.Context, record *kgo.Record) error {
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *kafkaProducer) Close() {
	p.client.Close()
}

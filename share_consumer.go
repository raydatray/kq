package kq

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type sharePollResult struct {
	records []*kgo.Record
	err     error
	closed  bool
}

type shareGroupConsumer interface {
	Poll(context.Context, int) sharePollResult
	Ack(*kgo.Record, kgo.AckStatus)
	FlushAcks(context.Context) error
	Close()
}

type kafkaShareGroupConsumer struct {
	client *kgo.Client
}

func newKafkaShareGroupConsumer(config WorkerConfig) (*kafkaShareGroupConsumer, error) {
	options, err := config.Config.kafkaOptions(
		kgo.ConsumeTopics(config.readyTopic()),
		kgo.ShareGroup(config.workerGroup()),
		kgo.ShareMaxRecords(int32(config.Concurrency)),
		kgo.ShareMaxRecordsStrict(),
	)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("kq: create share consumer: %w", err)
	}

	return &kafkaShareGroupConsumer{client: client}, nil
}

func (c *kafkaShareGroupConsumer) Poll(ctx context.Context, limit int) sharePollResult {
	fetches := c.client.PollRecords(ctx, limit)
	return sharePollResult{
		records: fetches.Records(),
		err:     fetches.Err(),
		closed:  fetches.IsClientClosed(),
	}
}

func (*kafkaShareGroupConsumer) Ack(record *kgo.Record, status kgo.AckStatus) {
	record.Ack(status)
}

func (c *kafkaShareGroupConsumer) FlushAcks(ctx context.Context) error {
	return c.client.FlushAcks(ctx)
}

func (c *kafkaShareGroupConsumer) Close() {
	c.client.Close()
}

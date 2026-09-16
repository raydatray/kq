package kq

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type sharePollResult struct {
	record *kgo.Record
	err    error
	closed bool
}

type shareGroupConsumer interface {
	Poll(context.Context) sharePollResult
	FlushAcks(context.Context) error
	Close()
}

type kafkaShareGroupConsumer struct {
	client *kgo.Client
}

func newKafkaShareGroupConsumer(config Config) (*kafkaShareGroupConsumer, error) {
	options, err := config.kafkaOptions(
		kgo.ConsumeTopics(config.readyTopic()),
		kgo.ShareGroup(config.workerGroup()),
		kgo.ShareMaxRecords(1),
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

func (c *kafkaShareGroupConsumer) Poll(ctx context.Context) sharePollResult {
	fetches := c.client.PollRecords(ctx, 1)
	records := fetches.Records()

	var record *kgo.Record
	if len(records) > 0 {
		record = records[0]
	}

	return sharePollResult{
		record: record,
		err:    fetches.Err(),
		closed: fetches.IsClientClosed(),
	}
}

func (c *kafkaShareGroupConsumer) FlushAcks(ctx context.Context) error {
	return c.client.FlushAcks(ctx)
}

func (c *kafkaShareGroupConsumer) Close() {
	c.client.Close()
}

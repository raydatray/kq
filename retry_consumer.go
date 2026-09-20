package kq

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type topicPartition struct {
	topic     string
	partition int32
}

type retryPollResult struct {
	record *kgo.Record
	err    error
	closed bool
}

type retryConsumer interface {
	Poll(context.Context) retryPollResult
	Pause(topicPartition)
	Resume(topicPartition)
	Commit(context.Context, *kgo.Record) error
	Close()
}

type kafkaRetryConsumer struct {
	client *kgo.Client
}

func newKafkaRetryConsumer(config Config) (*kafkaRetryConsumer, error) {
	topics := config.retryTopics()
	if len(topics) == 0 {
		return nil, errors.New("kq: retry grid is not configured")
	}

	options, err := config.kafkaOptions(
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(config.retryMoverGroup()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("kq: create retry consumer: %w", err)
	}

	return &kafkaRetryConsumer{client: client}, nil
}

func (c *kafkaRetryConsumer) Poll(ctx context.Context) retryPollResult {
	fetches := c.client.PollRecords(ctx, 1)
	records := fetches.Records()

	var record *kgo.Record
	if len(records) > 0 {
		record = records[0]
	}

	return retryPollResult{
		record: record,
		err:    fetches.Err(),
		closed: fetches.IsClientClosed(),
	}
}

func (c *kafkaRetryConsumer) Pause(partition topicPartition) {
	c.client.PauseFetchPartitions(map[string][]int32{
		partition.topic: {partition.partition},
	})
}

func (c *kafkaRetryConsumer) Resume(partition topicPartition) {
	c.client.ResumeFetchPartitions(map[string][]int32{
		partition.topic: {partition.partition},
	})
}

func (c *kafkaRetryConsumer) Commit(
	ctx context.Context,
	record *kgo.Record,
) error {
	return c.client.CommitRecords(ctx, record)
}

func (c *kafkaRetryConsumer) Close() {
	c.client.Close()
}

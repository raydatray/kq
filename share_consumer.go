package kq

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
	mu     sync.Mutex
	ackErr error
}

func newKafkaShareGroupConsumer(config WorkerConfig) (*kafkaShareGroupConsumer, error) {
	consumer := new(kafkaShareGroupConsumer)
	options, err := config.Config.kafkaOptions(
		kgo.ConsumeTopics(config.readyTopic()),
		kgo.ShareGroup(config.workerGroup()),
		kgo.ShareMaxRecords(int32(config.Concurrency)),
		kgo.ShareMaxRecordsStrict(),
		kgo.ShareAckCallback(consumer.recordAckResult),
	)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("kq: create share consumer: %w", err)
	}

	consumer.client = client
	return consumer, nil
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
	flushErr := c.client.FlushAcks(ctx)
	return errors.Join(flushErr, c.takeAckError())
}

func (c *kafkaShareGroupConsumer) takeAckError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ackErr := c.ackErr
	c.ackErr = nil
	return ackErr
}

func (c *kafkaShareGroupConsumer) Close() {
	c.client.Close()
}

func (c *kafkaShareGroupConsumer) recordAckResult(_ *kgo.Client, results kgo.ShareAckResults) {
	if err := results.Error(); err != nil {
		c.mu.Lock()
		c.ackErr = errors.Join(c.ackErr, err)
		c.mu.Unlock()
	}
}

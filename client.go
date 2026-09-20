package kq

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Client struct {
	config   Config
	producer recordProducer
}

func NewClient(config Config) (*Client, error) {
	producer, err := newKafkaProducer(config)
	if err != nil {
		return nil, err
	}

	return &Client{
		config:   config,
		producer: producer,
	}, nil
}

func (c *Client) Enqueue(ctx context.Context, task Task) (string, error) {
	envelope, err := newTaskEnvelope(
		task,
		c.config.RetryPolicy.maxRetries,
	)
	if err != nil {
		return "", err
	}

	value, err := encodeEnvelope(envelope)
	if err != nil {
		return "", err
	}

	err = c.producer.Produce(ctx, &kgo.Record{
		Topic: c.config.readyTopic(),
		Key:   []byte(envelope.Id),
		Value: value,
	})
	if err != nil {
		return "", fmt.Errorf("kq: enqueue task: %w", err)
	}

	return envelope.Id, nil
}

func (c *Client) Close() {
	c.producer.Close()
}

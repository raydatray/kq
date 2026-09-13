package kq

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type recordWriter interface {
	Write(context.Context, *kgo.Record) error
	Close()
}

type kafkaWriter struct {
	client *kgo.Client
}

func (k *kafkaWriter) Write(ctx context.Context, record *kgo.Record) error {
	return k.client.ProduceSync(ctx, record).FirstErr()
}

func (k *kafkaWriter) Close() {
	k.client.Close()
}

type Client struct {
	config Config
	writer recordWriter
}

func NewClient(config Config) (*Client, error) {
	options, err := config.kafkaOptions(
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("kq: create producer: %w", err)
	}

	return &Client{
		config: config,
		writer: &kafkaWriter{client: client},
	}, nil
}

func (c *Client) Enqueue(ctx context.Context, task Task) (string, error) {
	id, value, err := encodeTask(task)
	if err != nil {
		return "", err
	}

	err = c.writer.Write(ctx, &kgo.Record{
		Topic: c.config.readyTopic(),
		Key:   []byte(id),
		Value: value,
	})
	if err != nil {
		return "", fmt.Errorf("kq: enqueue task: %w", err)
	}

	return id, nil
}

func (c *Client) Close() {
	c.writer.Close()
}

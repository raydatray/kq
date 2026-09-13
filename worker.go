package kq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler func(context.Context, Task) error

type Worker struct {
	client  *kgo.Client
	handler Handler
}

func NewWorker(config Config, handler Handler) (*Worker, error) {
	if handler == nil {
		return nil, errors.New("kq: handler cannot be nil")
	}

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
		return nil, fmt.Errorf("kq: create worker consumer: %w", err)
	}

	return &Worker{
		client:  client,
		handler: handler,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		fetches := w.client.PollRecords(ctx, 1)
		records := fetches.Records()

		if len(records) == 0 {
			switch {
			case ctx.Err() != nil:
				return nil
			case fetches.IsClientClosed():
				return nil
			case fetches.Err() != nil:
				return fmt.Errorf("kq: poll ready queue: %w", fetches.Err())
			default:
				continue
			}
		}

		record := records[0]
		taskErr := w.handle(ctx, record.Value)

		status := kgo.AckAccept
		if taskErr != nil {
			status = kgo.AckRelease
		}
		record.Ack(status)

		ackErr := w.flushAcks()
		if taskErr != nil {
			return errors.Join(taskErr, ackErr)
		}
		if ackErr != nil {
			return fmt.Errorf("kq: ack task: %w", ackErr)
		}
	}
}

func (w *Worker) handle(ctx context.Context, value []byte) error {
	_, task, err := decodeTask(value)
	if err != nil {
		return err
	}

	return w.handler(ctx, task)
}

func (w *Worker) flushAcks() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	return w.client.FlushAcks(ctx)
}

func (w *Worker) Close() {
	w.client.Close()
}

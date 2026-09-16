package kq

import (
	"context"
	"errors"
	"fmt"
	"time"

	kqpb "github.com/raydatray/kq/internal/proto/kq"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler func(context.Context, Task) error

type Worker struct {
	consumer *kgo.Client
	writer   recordWriter
	config   Config
	handler  Handler
}

func NewWorker(config Config, handler Handler) (*Worker, error) {
	if handler == nil {
		return nil, errors.New("kq: handler cannot be nil")
	}

	consumerOptions, err := config.kafkaOptions(
		kgo.ConsumeTopics(config.readyTopic()),
		kgo.ShareGroup(config.workerGroup()),
		kgo.ShareMaxRecords(1),
		kgo.ShareMaxRecordsStrict(),
	)
	if err != nil {
		return nil, err
	}

	consumer, err := kgo.NewClient(consumerOptions...)
	if err != nil {
		return nil, fmt.Errorf("kq: create worker consumer: %w", err)
	}

	producerOptions, err := config.kafkaOptions(
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		consumer.Close()
		return nil, err
	}

	producer, err := kgo.NewClient(producerOptions...)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("kq: create retry producer: %w", err)
	}

	return &Worker{
		consumer: consumer,
		writer:   &kafkaWriter{client: producer},
		config:   config,
		handler:  handler,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		fetches := w.consumer.PollRecords(ctx, 1)
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
	envelope, err := decodeEnvelope(value)
	if err != nil {
		return err
	}

	handlerErr := w.handler(ctx, taskFromEnvelope(envelope))
	if handlerErr == nil {
		return nil
	}

	if envelope.Retried >= envelope.Retries {
		return errors.Join(errRetriesExhausted, handlerErr)
	}

	if err := w.scheduleRetry(ctx, envelope, handlerErr); err != nil {
		return errors.Join(handlerErr, err)
	}

	return nil
}

func (w *Worker) scheduleRetry(ctx context.Context, envelope *kqpb.TaskEnvelope, handlerErr error) error {
	retry := envelope.Retried + 1

	delay, err := w.config.RetryPolicy.retryAfter(retry)
	if err != nil {
		return err
	}
	bucket, err := w.config.RetryGrid.bucket(w.config.Queue, delay)
	if err != nil {
		return err
	}

	envelope.Retried = retry
	envelope.RetryAfter = durationpb.New(delay)
	envelope.LastError = handlerErr.Error()
	envelope.LastErrorAt = timestamppb.Now()

	value, err := encodeEnvelope(envelope)
	if err != nil {
		return err
	}

	return w.writer.Write(ctx, &kgo.Record{
		Topic:     bucket.topic,
		Partition: bucket.partition,
		Key:       []byte(envelope.Id),
		Value:     value,
	})
}

func (w *Worker) flushAcks() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	return w.consumer.FlushAcks(ctx)
}

func (w *Worker) Close() {
	w.consumer.Close()
	w.writer.Close()
}

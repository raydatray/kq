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
	consumer    shareGroupConsumer
	producer    recordProducer
	config      Config
	concurrency int
	handler     Handler
}

func NewWorker(config WorkerConfig, handler Handler) (*Worker, error) {
	if handler == nil {
		return nil, errors.New("kq: handler cannot be nil")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	consumer, err := newKafkaShareGroupConsumer(config)
	if err != nil {
		return nil, err
	}

	producer, err := newKafkaProducer(
		config.Config,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		consumer.Close()
		return nil, err
	}

	return &Worker{
		consumer:    consumer,
		producer:    producer,
		config:      config.Config,
		concurrency: config.Concurrency,
		handler:     handler,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		result := w.consumer.Poll(ctx, w.concurrency)

		if len(result.records) == 0 {
			switch {
			case ctx.Err() != nil:
				return nil
			case result.closed:
				return nil
			case result.err != nil:
				return fmt.Errorf("kq: poll ready queue: %w", result.err)
			default:
				continue
			}
		}

		var taskErr error
		for _, record := range result.records {
			err := w.handle(ctx, record.Value)
			status := kgo.AckAccept
			if err != nil {
				status = kgo.AckRelease
				taskErr = errors.Join(taskErr, err)
			}
			w.consumer.Ack(record, status)
		}

		ackErr := w.flushAcks()
		var pollErr error
		if result.err != nil {
			pollErr = fmt.Errorf("kq: poll ready queue: %w", result.err)
		}
		if ackErr != nil {
			ackErr = fmt.Errorf("kq: ack tasks: %w", ackErr)
		}
		if err := errors.Join(taskErr, ackErr, pollErr); err != nil {
			return err
		}
		if result.closed {
			return nil
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
		if err := w.deadLetter(ctx, envelope, handlerErr); err != nil {
			return errors.Join(handlerErr, err)
		}
		return nil
	}

	if err := w.scheduleRetry(ctx, envelope, handlerErr); err != nil {
		return errors.Join(handlerErr, err)
	}

	return nil
}

func (w *Worker) scheduleRetry(ctx context.Context, envelope *kqpb.TaskEnvelope, handlerErr error) error {
	retry := envelope.Retried + 1

	delay, err := w.config.RetryPolicy.retryAfter(retry, envelope.Id)
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

	return w.producer.Produce(ctx, &kgo.Record{
		Topic:     bucket.topic,
		Partition: bucket.partition,
		Key:       []byte(envelope.Id),
		Value:     value,
	})
}

func (w *Worker) deadLetter(ctx context.Context, envelope *kqpb.TaskEnvelope, cause error) error {
	envelope.LastError = cause.Error()
	envelope.LastErrorAt = timestamppb.Now()

	value, err := encodeEnvelope(envelope)
	if err != nil {
		return err
	}

	if err := w.producer.Produce(ctx, &kgo.Record{
		Topic:     w.config.deadLetterTopic(),
		Partition: 0, // mvp assumed dlq has 1 partition (we should probably make this a different producer that doesnt explicitly specify partition)
		Key:       []byte(envelope.Id),
		Value:     value,
	}); err != nil {
		return fmt.Errorf("kq: produce task to dlq: %w", err)
	}

	return nil
}

func (w *Worker) flushAcks() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	return w.consumer.FlushAcks(ctx)
}

func (w *Worker) Close() {
	w.consumer.Close()
	w.producer.Close()
}

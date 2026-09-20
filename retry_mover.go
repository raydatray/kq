package kq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type heldRetry struct {
	record *kgo.Record
	dueAt  time.Time
}

type RetryMover struct {
	config   Config
	consumer retryConsumer
	producer recordProducer
	held     map[topicPartition][]heldRetry
}

func NewRetryMover(config Config) (*RetryMover, error) {
	consumer, err := newKafkaRetryConsumer(config)
	if err != nil {
		return nil, err
	}

	producer, err := newKafkaProducer(config)
	if err != nil {
		consumer.Close()
		return nil, err
	}

	return &RetryMover{
		config:   config,
		consumer: consumer,
		producer: producer,
		held:     make(map[topicPartition][]heldRetry),
	}, nil
}

func (m *RetryMover) Run(ctx context.Context) error {
	for {
		moved, err := m.moveDueRecord(ctx)
		if err != nil {
			return err
		}
		if moved {
			continue
		}

		pollCtx, cancel := m.pollContext(ctx)
		result := m.consumer.Poll(pollCtx)
		pollErr := pollCtx.Err()
		cancel()

		if result.record != nil {
			if err := m.handleRecord(ctx, result.record); err != nil {
				return err
			}
			if result.err != nil {
				return fmt.Errorf("kq: poll retry topics: %w", result.err)
			}
			continue
		}

		switch {
		case ctx.Err() != nil:
			return nil
		case result.closed:
			return nil
		case errors.Is(pollErr, context.DeadlineExceeded):
			continue
		case result.err != nil:
			return fmt.Errorf("kq: poll retry topics: %w", result.err)
		}
	}
}

func (m *RetryMover) handleRecord(ctx context.Context, record *kgo.Record) error {
	envelope, err := decodeEnvelope(record.Value)
	if err != nil {
		return err
	}

	if err := envelope.RetryAfter.CheckValid(); err != nil {
		return fmt.Errorf("kq: invalid retry delay: %w", err)
	}

	retryAfter := envelope.RetryAfter.AsDuration()
	if retryAfter < 0 {
		return errors.New("kq: retry delay cannot be negative")
	}

	dueAt := record.Timestamp.Add(retryAfter)
	partition := topicPartition{
		topic:     record.Topic,
		partition: record.Partition,
	}
	held := m.held[partition]

	if len(held) > 0 || time.Now().Before(dueAt) {
		if len(held) == 0 {
			m.consumer.Pause(partition)
		}

		m.held[partition] = append(m.held[partition], heldRetry{
			record: record,
			dueAt:  dueAt,
		})
		return nil
	}

	return m.move(ctx, record)
}

func (m *RetryMover) moveDueRecord(ctx context.Context) (bool, error) {
	now := time.Now()

	for partition, records := range m.held {
		if len(records) == 0 || now.Before(records[0].dueAt) {
			continue
		}

		if err := m.move(ctx, records[0].record); err != nil {
			return false, err
		}

		records = records[1:]
		if len(records) == 0 {
			delete(m.held, partition)
			m.consumer.Resume(partition)
		} else {
			m.held[partition] = records
		}

		return true, nil
	}

	return false, nil
}

func (m *RetryMover) move(ctx context.Context, record *kgo.Record) error {
	// todo -  graceful shutdown should stop polling and allow an in-flight move
	// to finish before canceling its context or closing either Kafka client
	if err := m.producer.Produce(ctx, &kgo.Record{
		Topic: m.config.readyTopic(),
		Key:   record.Key,
		Value: record.Value,
	}); err != nil {
		return fmt.Errorf("kq: produce retry to ready: %w", err)
	}

	if err := m.consumer.Commit(ctx, record); err != nil {
		return fmt.Errorf("kq: commit retry record: %w", err)
	}

	return nil
}

func (m *RetryMover) pollContext(ctx context.Context) (context.Context, context.CancelFunc) {
	wakeAt := m.nextWake()
	if wakeAt.IsZero() {
		return ctx, func() {}
	}

	return context.WithDeadline(ctx, wakeAt)
}

func (m *RetryMover) nextWake() time.Time {
	var earliest time.Time

	for _, records := range m.held {
		if len(records) == 0 {
			continue
		}

		dueAt := records[0].dueAt
		if earliest.IsZero() || dueAt.Before(earliest) {
			earliest = dueAt
		}
	}

	return earliest
}

func (m *RetryMover) Close() {
	m.consumer.Close()
	m.producer.Close()
}

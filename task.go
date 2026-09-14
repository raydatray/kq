package kq

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"uuid"

	kqpb "github.com/raydatray/kq/internal/proto/kq"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Task struct {
	Type    string
	Payload []byte
}

func (t Task) validate() error {
	if strings.TrimSpace(t.Type) == "" {
		return errors.New("kq: task type cannot be empty")
	}

	return nil
}

func newTaskEnvelope(task Task, retries int32) (*kqpb.TaskEnvelope, error) {
	if err := task.validate(); err != nil {
		return nil, err
	}

	if retries < 0 {
		return nil, errors.New("kq: task retries cannot be negative")
	}

	return &kqpb.TaskEnvelope{
		Id:         uuid.NewV7().String(),
		Type:       task.Type,
		Payload:    bytes.Clone(task.Payload),
		EnqueuedAt: timestamppb.Now(),
		Retries:    retries,
	}, nil
}

func encodeEnvelope(envelope *kqpb.TaskEnvelope) ([]byte, error) {
	value, err := proto.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("kq: encode task envelope: %w", err)
	}
	return value, nil
}

func decodeEnvelope(value []byte) (*kqpb.TaskEnvelope, error) {
	var envelope kqpb.TaskEnvelope
	if err := proto.Unmarshal(value, &envelope); err != nil {
		return nil, fmt.Errorf("kq: decode task envelope: %w", err)
	}

	if strings.TrimSpace(envelope.Id) == "" {
		return nil, errors.New("kq: task ID cannot be empty")
	}
	if strings.TrimSpace(envelope.Type) == "" {
		return nil, errors.New("kq: task type cannot be empty")
	}
	if envelope.EnqueuedAt == nil {
		return nil, errors.New("kq: enqueue time cannot be empty")
	}
	if err := envelope.EnqueuedAt.CheckValid(); err != nil {
		return nil, fmt.Errorf("kq: invalid enqueue time: %w", err)
	}

	return &envelope, nil
}

func taskFromEnvelope(envelope *kqpb.TaskEnvelope) Task {
	return Task{
		Type:    envelope.Type,
		Payload: bytes.Clone(envelope.Payload),
	}
}

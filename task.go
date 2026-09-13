package kq

import (
	"errors"
	"fmt"
	"strings"
	"uuid"

	"github.com/raydatray/kq/internal/kqpb"
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

func encodeTask(task Task) (string, []byte, error) {
	if err := task.validate(); err != nil {
		return "", nil, err
	}

	id := uuid.NewV7().String()

	value, err := proto.Marshal(&kqpb.TaskEnvelope{
		Id:         id,
		Type:       task.Type,
		Payload:    task.Payload,
		EnqueuedAt: timestamppb.Now(),
	})
	if err != nil {
		return "", nil, fmt.Errorf("kq: encode task: %w", err)
	}

	return id, value, nil
}

func decodeTask(value []byte) (string, Task, error) {
	var envelope kqpb.TaskEnvelope

	if err := proto.Unmarshal(value, &envelope); err != nil {
		return "", Task{}, fmt.Errorf("kq: decode task: %w", err)
	}

	if strings.TrimSpace(envelope.Id) == "" {
		return "", Task{}, errors.New("kq: task ID cannot be empty")
	}
	if strings.TrimSpace(envelope.Type) == "" {
		return "", Task{}, errors.New("kq: task type cannot be empty")
	}
	if envelope.EnqueuedAt == nil {
		return "", Task{}, errors.New("kq: enqueue time cannot be empty")
	}
	if err := envelope.EnqueuedAt.CheckValid(); err != nil {
		return "", Task{}, fmt.Errorf("kq: invalid enqueue time: %w", err)
	}

	return envelope.Id, Task{
		Type:    envelope.Type,
		Payload: append([]byte(nil), envelope.Payload...),
	}, nil
}

package kq

import (
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers []string
	Queue   string

	RetryPolicy RetryPolicy
	RetryGrid   RetryGrid
}

func (c Config) kafkaOptions(options ...kgo.Opt) ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("kq: at least one broker is required")
	}
	for _, broker := range c.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, errors.New("kq: broker cannot be empty")
		}
	}
	if strings.TrimSpace(c.Queue) == "" {
		return nil, errors.New("kq: queue name cannot be empty")
	}

	result := []kgo.Opt{kgo.SeedBrokers(c.Brokers...)}
	return append(result, options...), nil
}

func (c Config) readyTopic() string {
	return c.Queue + "-ready"
}

func (c Config) workerGroup() string {
	return "kq." + c.Queue + ".workers"
}

func (c Config) retryTopics() []string {
	if len(c.RetryGrid.boundaries) < 2 {
		return nil
	}

	topics := make([]string, 0, len(c.RetryGrid.boundaries)-1)
	for _, lower := range c.RetryGrid.boundaries[:len(c.RetryGrid.boundaries)-1] {
		topics = append(topics, retryTopic(c.Queue, lower))
	}

	return topics
}

func (c Config) retryMoverGroup() string {
	return "kq." + c.Queue + ".retry-mover"
}

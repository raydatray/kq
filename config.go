package kq

import (
	"errors"
	"fmt"
	"strings"
	"time"

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
	if err := c.validateRetries(); err != nil {
		return nil, err
	}

	result := []kgo.Opt{kgo.SeedBrokers(c.Brokers...)}
	return append(result, options...), nil
}

func (c Config) validateRetries() error {
	if c.RetryPolicy.maxRetries == 0 {
		return nil
	}
	if len(c.RetryGrid.boundaries) < 2 || c.RetryGrid.partitions <= 0 {
		return errors.New("kq: retry grid is not configured")
	}

	return nil
}

func (c Config) readyTopic() string {
	return c.Queue + "-ready"
}

func (c Config) workerGroup() string {
	return "kq." + c.Queue + ".workers"
}

func retryTopic(queue string, lower time.Duration) string {
	return fmt.Sprintf("%s-retry-%s", queue, formatRetryDelay(lower))
}

func formatRetryDelay(delay time.Duration) string {
	switch {
	case delay == 0:
		return "0s"
	case delay%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", delay/(24*time.Hour))
	case delay%time.Hour == 0:
		return fmt.Sprintf("%dh", delay/time.Hour)
	case delay%time.Minute == 0:
		return fmt.Sprintf("%dm", delay/time.Minute)
	default:
		return fmt.Sprintf("%ds", delay/time.Second)
	}
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

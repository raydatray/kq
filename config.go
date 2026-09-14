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
}

func (c Config) kafkaOptions(options ...kgo.Opt) ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("kq: atleast one broker is required")
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

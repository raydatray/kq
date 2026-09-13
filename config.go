package kq

import (
	"errors"
	"strings"

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

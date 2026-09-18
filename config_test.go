package kq

import (
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConfigKafkaOptionsValidation(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name:   "no brokers",
			config: Config{Queue: "email"},
			want:   "kq: at least one broker is required",
		},
		{
			name:   "empty broker",
			config: Config{Brokers: []string{"localhost:9092", " "}, Queue: "email"},
			want:   "kq: broker cannot be empty",
		},
		{
			name:   "empty queue",
			config: Config{Brokers: []string{"localhost:9092"}},
			want:   "kq: queue name cannot be empty",
		},
		{
			name:   "blank queue",
			config: Config{Brokers: []string{"localhost:9092"}, Queue: " "},
			want:   "kq: queue name cannot be empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.config.kafkaOptions()
			if err == nil || err.Error() != test.want {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestConfigKafkaOptions(t *testing.T) {
	config := Config{
		Brokers: []string{"localhost:9092"},
		Queue:   "email",
	}

	options, err := config.kafkaOptions(kgo.ClientID("test"))
	if err != nil {
		t.Fatal(err)
	}
	if len(options) != 2 {
		t.Fatalf("options length = %d, want 2", len(options))
	}
}

func TestConfigNames(t *testing.T) {
	config := Config{Queue: "email"}

	if got := config.readyTopic(); got != "email-ready" {
		t.Errorf("ready topic = %q, want %q", got, "email-ready")
	}
	if got := config.workerGroup(); got != "kq.email.workers" {
		t.Errorf("worker group = %q, want %q", got, "kq.email.workers")
	}
	if got := config.retryMoverGroup(); got != "kq.email.retry-mover" {
		t.Errorf("retry mover group = %q, want %q", got, "kq.email.retry-mover")
	}
	if got := config.deadLetterTopic(); got != "email-dlq" {
		t.Errorf("dead letter topic = %q, want %q", got, "email-dlq")
	}
}

func TestConfigRetryTopics(t *testing.T) {
	grid, err := NewRetryGrid([]time.Duration{
		0,
		2 * time.Minute,
		8 * time.Minute,
		24 * time.Hour,
	}, 4)
	if err != nil {
		t.Fatal(err)
	}

	config := Config{Queue: "email", RetryGrid: grid}
	want := []string{
		"email-retry-0s",
		"email-retry-2m",
		"email-retry-8m",
	}

	if got := config.retryTopics(); !reflect.DeepEqual(got, want) {
		t.Fatalf("retry topics = %v, want %v", got, want)
	}
}

func TestConfigRetryTopicsWithoutGrid(t *testing.T) {
	if topics := (Config{Queue: "email"}).retryTopics(); topics != nil {
		t.Fatalf("retry topics = %v, want nil", topics)
	}
}

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

func TestConfigRetryValidation(t *testing.T) {
	grid, err := NewRetryGrid([]time.Duration{0, 2 * time.Minute}, 4)
	if err != nil {
		t.Fatal(err)
	}
	delay := func(int32) time.Duration { return time.Minute }

	tests := []struct {
		name   string
		policy RetryPolicy
		grid   RetryGrid
		want   string
	}{
		{name: "disabled"},
		{name: "negative maximum", policy: NewRetryPolicy(-1, delay), want: "kq: maximum retries cannot be negative"},
		{name: "missing delay", policy: NewRetryPolicy(1, nil), grid: grid, want: "kq: retry delay function is not configured"},
		{name: "missing grid", policy: NewRetryPolicy(1, delay), want: "kq: retry grid is not configured"},
		{name: "enabled", policy: NewRetryPolicy(1, delay), grid: grid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{
				Brokers:     []string{"localhost:9092"},
				Queue:       "email",
				RetryPolicy: test.policy,
				RetryGrid:   test.grid,
			}

			_, err := config.kafkaOptions()
			switch {
			case test.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case test.want != "" && (err == nil || err.Error() != test.want):
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
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

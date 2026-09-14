package kq

import (
	"errors"
	"testing"
	"time"
)

func TestNewRetryGridValidation(t *testing.T) {
	tests := []struct {
		name       string
		boundaries []time.Duration
		partitions int32
	}{
		{name: "no boundaries", partitions: 1},
		{name: "one boundary", boundaries: []time.Duration{0}, partitions: 1},
		{name: "does not start at zero", boundaries: []time.Duration{time.Second, 2 * time.Second}, partitions: 1},
		{name: "zero partitions", boundaries: []time.Duration{0, time.Second}},
		{name: "negative partitions", boundaries: []time.Duration{0, time.Second}, partitions: -1},
		{name: "duplicate boundary", boundaries: []time.Duration{0, time.Minute, time.Minute}, partitions: 1},
		{name: "decreasing boundary", boundaries: []time.Duration{0, time.Minute, 30 * time.Second}, partitions: 1},
		{name: "fractional second boundary", boundaries: []time.Duration{0, 1500 * time.Millisecond}, partitions: 1},
		{name: "uneven partitions", boundaries: []time.Duration{0, 5 * time.Second}, partitions: 2},
		{name: "sub-second partition range", boundaries: []time.Duration{0, 2 * time.Second}, partitions: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRetryGrid(test.boundaries, test.partitions); err == nil {
				t.Fatal("NewRetryGrid() error = nil")
			}
		})
	}
}

func TestNewRetryGridCopiesBoundaries(t *testing.T) {
	boundaries := []time.Duration{0, 2 * time.Minute}
	grid, err := NewRetryGrid(boundaries, 4)
	if err != nil {
		t.Fatal(err)
	}

	boundaries[1] = time.Hour

	bucket, err := grid.bucket("email", 70*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if bucket.upperBound != 90*time.Second {
		t.Fatalf("upper bound = %s, want 1m30s", bucket.upperBound)
	}
}

func TestRetryGridBucket(t *testing.T) {
	grid, err := NewRetryGrid([]time.Duration{
		0,
		2 * time.Minute,
		8 * time.Minute,
	}, 4)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		delay      time.Duration
		topic      string
		partition  int32
		upperBound time.Duration
	}{
		{name: "zero", topic: "email-retry-0s", upperBound: 30 * time.Second},
		{name: "first boundary", delay: 30 * time.Second, topic: "email-retry-0s", upperBound: 30 * time.Second},
		{name: "above first boundary", delay: 30*time.Second + 1, topic: "email-retry-0s", partition: 1, upperBound: time.Minute},
		{name: "first band", delay: 70 * time.Second, topic: "email-retry-0s", partition: 2, upperBound: 90 * time.Second},
		{name: "shared boundary", delay: 2 * time.Minute, topic: "email-retry-0s", partition: 3, upperBound: 2 * time.Minute},
		{name: "above shared boundary", delay: 2*time.Minute + 1, topic: "email-retry-2m", upperBound: 210 * time.Second},
		{name: "second band", delay: 4 * time.Minute, topic: "email-retry-2m", partition: 1, upperBound: 5 * time.Minute},
		{name: "maximum", delay: 8 * time.Minute, topic: "email-retry-2m", partition: 3, upperBound: 8 * time.Minute},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bucket, err := grid.bucket("email", test.delay)
			if err != nil {
				t.Fatal(err)
			}
			if bucket.topic != test.topic || bucket.partition != test.partition || bucket.upperBound != test.upperBound {
				t.Fatalf("bucket(%s) = %+v, want topic %q, partition %d, upper bound %s", test.delay, bucket, test.topic, test.partition, test.upperBound)
			}
		})
	}
}

func TestRetryGridDelayOutOfRange(t *testing.T) {
	grid, err := NewRetryGrid([]time.Duration{0, 8 * time.Minute}, 4)
	if err != nil {
		t.Fatal(err)
	}

	for _, delay := range []time.Duration{-time.Second, 8*time.Minute + time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			_, err := grid.bucket("email", delay)
			if !errors.Is(err, ErrRetryDelayOutOfRange) {
				t.Fatalf("error = %v, want ErrRetryDelayOutOfRange", err)
			}
		})
	}
}

func TestRetryTopic(t *testing.T) {
	tests := []struct {
		delay time.Duration
		want  string
	}{
		{want: "email-retry-0s"},
		{delay: 30 * time.Second, want: "email-retry-30s"},
		{delay: 2 * time.Minute, want: "email-retry-2m"},
		{delay: 8 * time.Hour, want: "email-retry-8h"},
		{delay: 48 * time.Hour, want: "email-retry-2d"},
	}

	for _, test := range tests {
		if got := retryTopic("email", test.delay); got != test.want {
			t.Errorf("retryTopic(email, %s) = %q, want %q", test.delay, got, test.want)
		}
	}
}

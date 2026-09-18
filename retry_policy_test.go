package kq

import (
	"errors"
	"testing"
	"time"
)

func TestRetryPolicyRetryAfter(t *testing.T) {
	policy := NewRetryPolicy(3, func(retry int32) time.Duration {
		return time.Duration(retry) * time.Minute
	})

	tests := []struct {
		retry int32
		want  time.Duration
	}{
		{retry: 1, want: time.Minute},
		{retry: 2, want: 2 * time.Minute},
		{retry: 3, want: 3 * time.Minute},
	}

	for _, test := range tests {
		got, err := policy.retryAfter(test.retry)
		if err != nil {
			t.Errorf("retryAfter(%d): %v", test.retry, err)
		} else if got != test.want {
			t.Errorf("retryAfter(%d) = %s, want %s", test.retry, got, test.want)
		}
	}
}

func TestRetryPolicyRejectsInvalidRetryNumber(t *testing.T) {
	policy := NewRetryPolicy(3, func(int32) time.Duration { return time.Minute })

	if _, err := policy.retryAfter(0); err == nil {
		t.Fatal("zero retry number accepted")
	}
	if _, err := policy.retryAfter(4); !errors.Is(err, errRetriesExhausted) {
		t.Fatalf("error = %v, want errRetriesExhausted", err)
	}
}

func TestRetryPolicyWithoutDelay(t *testing.T) {
	policy := NewRetryPolicy(1, nil)

	if _, err := policy.retryAfter(1); err == nil {
		t.Fatal("nil retry delay function accepted")
	}
}

func TestRetryPolicyRejectsNegativeDelay(t *testing.T) {
	policy := NewRetryPolicy(1, func(int32) time.Duration { return -time.Second })

	if _, err := policy.retryAfter(1); err == nil {
		t.Fatal("negative retry delay accepted")
	}
}

func TestJitteredQuarticBackoff(t *testing.T) {
	for retry := int32(1); retry <= 6; retry++ {
		n := int64(retry - 1)
		base := n*n*n*n + 15
		step := n + 1
		maximum := base + 29*step

		for range 100 {
			delay := JitteredQuarticBackoff(retry)
			if delay%time.Second != 0 {
				t.Fatalf("retry %d delay %s is not a whole number of seconds", retry, delay)
			}

			seconds := int64(delay / time.Second)
			if seconds < base || seconds > maximum {
				t.Fatalf("retry %d delay = %s, want between %ds and %ds", retry, delay, base, maximum)
			}
			if (seconds-base)%step != 0 {
				t.Fatalf("retry %d delay = %s, want jitter in %ds steps", retry, delay, step)
			}
		}
	}
}

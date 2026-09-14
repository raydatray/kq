package kq

import (
	"errors"
	"testing"
	"time"
)

func TestRetryPolicyRetryAfter(t *testing.T) {
	policy := newTestRetryPolicy(t, 3, func(retry int32, _ string) time.Duration {
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
		got, err := policy.retryAfter(test.retry, "task-id")
		if err != nil {
			t.Errorf("retryAfter(%d): %v", test.retry, err)
		} else if got != test.want {
			t.Errorf("retryAfter(%d) = %s, want %s", test.retry, got, test.want)
		}
	}
}

func TestRetryPolicyRejectsInvalidRetryNumber(t *testing.T) {
	policy := newTestRetryPolicy(t, 3, func(int32, string) time.Duration { return time.Minute })

	if _, err := policy.retryAfter(0, "task-id"); err == nil {
		t.Fatal("zero retry number accepted")
	}
	if _, err := policy.retryAfter(4, "task-id"); !errors.Is(err, errRetriesExhausted) {
		t.Fatalf("error = %v, want errRetriesExhausted", err)
	}
}

func TestNewRetryPolicyValidation(t *testing.T) {
	delay := func(int32, string) time.Duration { return time.Minute }
	tests := []struct {
		name       string
		maxRetries int32
		delay      RetryDelayFunc
		want       string
	}{
		{name: "disabled"},
		{name: "negative maximum", maxRetries: -1, delay: delay, want: "kq: maximum retries cannot be negative"},
		{name: "missing delay", maxRetries: 1, want: "kq: retry delay function is not configured"},
		{name: "enabled", maxRetries: 1, delay: delay},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewRetryPolicy(test.maxRetries, test.delay)
			switch {
			case test.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case test.want != "" && (err == nil || err.Error() != test.want):
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRetryPolicyRejectsNegativeDelay(t *testing.T) {
	policy := newTestRetryPolicy(t, 1, func(int32, string) time.Duration { return -time.Second })

	if _, err := policy.retryAfter(1, "task-id"); err == nil {
		t.Fatal("negative retry delay accepted")
	}
}

func newTestRetryPolicy(t *testing.T, maxRetries int32, delay RetryDelayFunc) RetryPolicy {
	t.Helper()

	policy, err := NewRetryPolicy(maxRetries, delay)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestJitteredQuarticBackoff(t *testing.T) {
	taskIDs := []string{
		"01995a9e-b93c-7000-8000-000000000001",
		"01995a9e-b93c-7000-8000-000000000002",
		"01995a9e-b93c-7000-8000-000000000003",
	}

	for retry := int32(1); retry <= 6; retry++ {
		n := int64(retry - 1)
		base := n*n*n*n + 15
		maximum := base + 29*(n+1)

		for _, taskID := range taskIDs {
			delay := JitteredQuarticBackoff(retry, taskID)
			if delay%time.Second != 0 {
				t.Fatalf("retry %d delay %s is not a whole number of seconds", retry, delay)
			}

			seconds := int64(delay / time.Second)
			if seconds < base || seconds > maximum {
				t.Fatalf("retry %d delay = %s, want between %ds and %ds", retry, delay, base, maximum)
			}

			for range 100 {
				if got := JitteredQuarticBackoff(retry, taskID); got != delay {
					t.Fatalf("retry %d task %q delay changed from %s to %s", retry, taskID, delay, got)
				}
			}
		}
	}
}

func TestJitteredQuarticBackoffStable(t *testing.T) {
	tests := []struct {
		taskID string
		retry  int32
		want   time.Duration
	}{
		{taskID: "01995a9e-b93c-7000-8000-000000000001", retry: 1, want: 31 * time.Second},
		{taskID: "01995a9e-b93c-7000-8000-000000000002", retry: 1, want: 40 * time.Second},
		{taskID: "01995a9e-b93c-7000-8000-000000000001", retry: 6, want: 706 * time.Second},
	}

	for _, test := range tests {
		if got := JitteredQuarticBackoff(test.retry, test.taskID); got != test.want {
			t.Errorf("retry %d task %q delay = %s, want %s", test.retry, test.taskID, got, test.want)
		}
	}
}

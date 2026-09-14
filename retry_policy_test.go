package kq

import (
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
		ok    bool
	}{
		{retry: -1},
		{retry: 0},
		{retry: 1, want: time.Minute, ok: true},
		{retry: 2, want: 2 * time.Minute, ok: true},
		{retry: 3, want: 3 * time.Minute, ok: true},
		{retry: 4},
	}

	for _, test := range tests {
		got, ok := policy.retryAfter(test.retry)
		if got != test.want || ok != test.ok {
			t.Errorf("retryAfter(%d) = (%s, %t), want (%s, %t)", test.retry, got, ok, test.want, test.ok)
		}
	}
}

func TestRetryPolicyWithoutDelay(t *testing.T) {
	policy := NewRetryPolicy(1, nil)

	if delay, ok := policy.retryAfter(1); delay != 0 || ok {
		t.Fatalf("retryAfter(1) = (%s, %t), want (0s, false)", delay, ok)
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

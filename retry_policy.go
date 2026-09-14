package kq

import (
	"errors"
	"math/rand/v2"
	"time"
)

var errRetriesExhausted = errors.New("kq: retries exhausted")

type RetryDelayFunc func(retry int32) time.Duration

type RetryPolicy struct {
	maxRetries int32
	delay      RetryDelayFunc
}

func NewRetryPolicy(maxRetries int32, delay RetryDelayFunc) RetryPolicy {
	return RetryPolicy{
		maxRetries: maxRetries,
		delay:      delay,
	}
}

func (p RetryPolicy) retryAfter(retry int32) (time.Duration, error) {
	if retry < 1 {
		return 0, errors.New("kq: retry number must be positive")
	}
	if retry > p.maxRetries {
		return 0, errRetriesExhausted
	}
	if p.delay == nil {
		return 0, errors.New("kq: retry delay function is not configured")
	}

	delay := p.delay(retry)
	if delay < 0 {
		return 0, errors.New("kq: retry delay cannot be negative")
	}

	return delay, nil
}

func JitteredQuarticBackoff(retry int32) time.Duration {
	n := int(retry - 1)
	seconds := n*n*n*n + 15 + rand.IntN(30)*(n+1)
	return time.Duration(seconds) * time.Second
}

package kq

import (
	"math/rand/v2"
	"time"
)

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

func (p RetryPolicy) retryAfter(retry int32) (time.Duration, bool) {
	if retry < 1 || retry > p.maxRetries || p.delay == nil {
		return 0, false
	}
	return p.delay(retry), true
}

func JitteredQuarticBackoff(retry int32) time.Duration {
	n := int(retry - 1)
	seconds := n*n*n*n + 15 + rand.IntN(30)*(n+1)
	return time.Duration(seconds) * time.Second
}

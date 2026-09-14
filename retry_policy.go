package kq

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"time"
)

var errRetriesExhausted = errors.New("kq: retries exhausted")

type RetryDelayFunc func(retry int32, taskID string) time.Duration

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

func (p RetryPolicy) retryAfter(retry int32, taskID string) (time.Duration, error) {
	if retry < 1 {
		return 0, errors.New("kq: retry number must be positive")
	}
	if retry > p.maxRetries {
		return 0, errRetriesExhausted
	}
	if p.delay == nil {
		return 0, errors.New("kq: retry delay function is not configured")
	}

	delay := p.delay(retry, taskID)
	if delay < 0 {
		return 0, errors.New("kq: retry delay cannot be negative")
	}

	return delay, nil
}

func JitteredQuarticBackoff(retry int32, taskID string) time.Duration {
	n := int64(retry - 1)
	base := n*n*n*n + 15
	jitterMax := uint64(29 * (n + 1))
	jitter := stableRetryHash(taskID, retry) % (jitterMax + 1)
	seconds := base + int64(jitter)
	return time.Duration(seconds) * time.Second
}

func stableRetryHash(taskID string, retry int32) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(taskID))

	var retryBytes [4]byte
	binary.LittleEndian.PutUint32(retryBytes[:], uint32(retry))
	_, _ = hash.Write(retryBytes[:])
	return hash.Sum64()
}

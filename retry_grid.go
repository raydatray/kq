package kq

import (
	"errors"
	"fmt"
	"time"
)

var ErrRetryDelayOutOfRange = errors.New("kq: retry delay outside grid")

type RetryGrid struct {
	boundaries []time.Duration
	partitions int32
}

func NewRetryGrid(boundaries []time.Duration, partitions int32) (RetryGrid, error) {
	if len(boundaries) < 2 {
		return RetryGrid{}, errors.New("kq: retry grid requires at least two boundaries")
	}
	if boundaries[0] != 0 {
		return RetryGrid{}, errors.New("kq: retry grid must start at zero")
	}
	if partitions <= 0 {
		return RetryGrid{}, errors.New("kq: retry grid partitions must be positive")
	}

	for i, boundary := range boundaries {
		if boundary%time.Second != 0 {
			return RetryGrid{}, fmt.Errorf("kq: retry grid boundary %s must be a whole number of seconds", boundary)
		}
		if i == 0 {
			continue
		}

		previous := boundaries[i-1]
		if boundary <= previous {
			return RetryGrid{}, errors.New("kq: retry grid boundaries must be strictly increasing")
		}

		span := boundary - previous
		width := span / time.Duration(partitions)
		if span%time.Duration(partitions) != 0 || width%time.Second != 0 {
			return RetryGrid{}, fmt.Errorf("kq: retry grid band %s-%s must divide evenly across %d partitions", previous, boundary, partitions)
		}
		if width < time.Second {
			return RetryGrid{}, errors.New("kq: retry grid partition ranges must be at least one second")
		}
	}

	return RetryGrid{
		boundaries: append([]time.Duration(nil), boundaries...),
		partitions: partitions,
	}, nil
}

type retryBucket struct {
	topic      string
	partition  int32
	upperBound time.Duration
}

func (g RetryGrid) bucket(queue string, delay time.Duration) (retryBucket, error) {
	if len(g.boundaries) < 2 || g.partitions <= 0 {
		return retryBucket{}, errors.New("kq: retry grid is not configured")
	}

	if delay < g.boundaries[0] || delay > g.boundaries[len(g.boundaries)-1] {
		return retryBucket{}, ErrRetryDelayOutOfRange
	}

	for band := 0; band < len(g.boundaries)-1; band++ {
		lower := g.boundaries[band]
		upper := g.boundaries[band+1]

		if delay > upper {
			continue
		}

		width := (upper - lower) / time.Duration(g.partitions)
		relative := max(delay-lower, 0)

		partition := int32(0)
		if relative > 0 {
			partition = int32((relative - 1) / width)
		}

		return retryBucket{
			topic:      retryTopic(queue, lower),
			partition:  partition,
			upperBound: lower + time.Duration(partition+1)*width,
		}, nil
	}

	return retryBucket{}, ErrRetryDelayOutOfRange
}

func retryTopic(queue string, lower time.Duration) string {
	return fmt.Sprintf("%s-retry-%s", queue, formatRetryDelay(lower))
}

func formatRetryDelay(delay time.Duration) string {
	switch {
	case delay == 0:
		return "0s"
	case delay%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", delay/(24*time.Hour))
	case delay%time.Hour == 0:
		return fmt.Sprintf("%dh", delay/time.Hour)
	case delay%time.Minute == 0:
		return fmt.Sprintf("%dm", delay/time.Minute)
	default:
		return fmt.Sprintf("%ds", delay/time.Second)
	}
}

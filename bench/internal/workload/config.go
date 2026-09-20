package workload

import (
	"errors"
	"math/rand/v2"
	"time"
)

type Config struct {
	RandomSeed    uint64               `json:"random_seed"`
	ExecutionTime DurationDistribution `json:"execution_time_ms"`
	Failures      FailureConfig        `json:"failures"`
}

type FailureMode string

const (
	FailureModeNone      FailureMode = "none"
	FailureModePermanent FailureMode = "permanent"
	FailureModeUntilTime FailureMode = "until_time"
	FailureModeAttempts  FailureMode = "attempts"
)

type FailureConfig struct {
	Rate       float64     `json:"rate"`
	Mode       FailureMode `json:"mode"`
	DurationMS int64       `json:"duration_ms,omitempty"`
	Attempts   int         `json:"attempts,omitempty"`
	Until      time.Time   `json:"until,omitempty"`
}

func (f FailureConfig) ModeFor(rng *rand.Rand) FailureMode {
	if f.Mode == "" || f.Mode == FailureModeNone {
		return FailureModeNone
	}
	if f.Rate <= 0 {
		return FailureModeNone
	}
	if f.Rate >= 1 {
		return f.Mode
	}
	if rng.Float64() < f.Rate {
		return f.Mode
	}
	return FailureModeNone
}

func (c Config) Validate() error {
	if err := c.ExecutionTime.Validate(); err != nil {
		return err
	}
	return c.Failures.Validate()
}

func (f FailureConfig) Validate() error {
	switch f.Mode {
	case "", FailureModeNone, FailureModePermanent, FailureModeUntilTime, FailureModeAttempts:
	default:
		return errors.New("workload: unknown failure mode")
	}
	if f.Rate < 0 || f.Rate > 1 {
		return errors.New("workload: failure rate must be between 0 and 1")
	}
	if f.DurationMS < 0 {
		return errors.New("workload: failure duration cannot be negative")
	}
	if f.Attempts < 0 {
		return errors.New("workload: failure attempts cannot be negative")
	}
	return nil
}

package workload

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Config describes deterministic workload generation.
//
// Generation depends only on the random seed, the workload ID, and this
// configuration. Changing producer count must not change the workload
// generated for a given ID.
type Config struct {
	RandomSeed    uint64               `json:"random_seed"`
	ExecutionTime DurationDistribution `json:"execution_time_ms"`
	Failures      FailureConfig        `json:"failures"`
}

// FailureConfig selects which workload IDs fail and how they fail.
type FailureConfig struct {
	Rate       float64   `json:"rate"`
	Mode       string    `json:"mode"`
	DurationMS int64     `json:"duration_ms,omitempty"`
	Attempts   int       `json:"attempts,omitempty"`
	Until      time.Time `json:"until,omitempty"`
}

// ModeFor deterministically selects the failure mode for one workload ID.
//
// A uniform draw below Rate selects the configured mode; otherwise the task
// succeeds. The "none" mode always succeeds regardless of Rate.
func (f FailureConfig) ModeFor(rng *rand.Rand) string {
	if f.Mode == "" || f.Mode == "none" {
		return "none"
	}
	if f.Rate <= 0 {
		return "none"
	}
	if f.Rate >= 1 {
		return f.Mode
	}
	if rng.Float64() < f.Rate {
		return f.Mode
	}
	return "none"
}

// Validate checks failure and execution-time configuration.
func (c Config) Validate() error {
	if err := c.ExecutionTime.Validate(); err != nil {
		return err
	}
	return c.Failures.Validate()
}

// Validate checks failure configuration.
func (f FailureConfig) Validate() error {
	switch f.Mode {
	case "", "none", "permanent", "until_time", "attempts":
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

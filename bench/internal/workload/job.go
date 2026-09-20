package workload

import (
	"math/rand/v2"
	"time"
)

// Job is the harness payload stored in the task envelope.
//
// WorkloadID is stable across retries and independent of KQ's internal task
// ID, allowing correctness accounting. EnqueuedAt is set by the producer at
// enqueue time; Generate leaves it zero so generation stays a pure function
// of configuration and workload ID.
type Job struct {
	WorkloadID               uint64    `json:"workload_id"`
	EnqueuedAt               time.Time `json:"enqueued_at"`
	SimulatedExecutionTimeMS int64     `json:"simulated_execution_time_ms"`
	FailureMode              string    `json:"failure_mode"`
	FailUntil                time.Time `json:"fail_until,omitempty"`
	FailAttempts             int       `json:"fail_attempts,omitempty"`
}

// Generate returns the deterministic job for a workload ID.
//
// The RNG is seeded with (RandomSeed, workloadID) via rand.NewPCG, so the
// same ID always yields the same execution time and failure selection
// regardless of producer count or ordering. Sample consumes the first draw;
// ModeFor consumes the second.
func Generate(config Config, workloadID uint64) Job {
	rng := rand.New(rand.NewPCG(config.RandomSeed, workloadID))

	return Job{
		WorkloadID:               workloadID,
		SimulatedExecutionTimeMS: config.ExecutionTime.Sample(rng).Milliseconds(),
		FailureMode:              config.Failures.ModeFor(rng),
		FailUntil:                config.Failures.Until,
		FailAttempts:             config.Failures.Attempts,
	}
}

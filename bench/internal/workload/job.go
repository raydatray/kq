package workload

import (
	"math/rand/v2"
	"time"
)

type Job struct {
	WorkloadID               uint64    `json:"workload_id"`
	EnqueuedAt               time.Time `json:"enqueued_at"`
	SimulatedExecutionTimeMS int64     `json:"simulated_execution_time_ms"`
	FailureMode              string    `json:"failure_mode"`
	FailUntil                time.Time `json:"fail_until,omitempty"`
	FailAttempts             int       `json:"fail_attempts,omitempty"`
}

// Generation is pure in config and workload ID, independent of producer assignment.
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

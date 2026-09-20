// Package events defines the newline-delimited JSON protocol spoken by
// kq-load role processes and consumed by the Python harness.
package events

import "time"

// Event types emitted by role processes.
const (
	TypeProcessReady    = "process_ready"
	TypeEnqueueStarted  = "enqueue_started"
	TypeEnqueueFinished = "enqueue_finished"
	TypeHandlerStarted  = "handler_started"
	TypeHandlerFinished = "handler_finished"
	TypeDeadLettered    = "dead_lettered"
	TypeRetryMoved      = "retry_moved"
	TypeProcessError    = "process_error"
	TypeProcessStopped  = "process_stopped"
)

// Roles.
const (
	RoleProducer = "producer"
	RoleWorker   = "worker"
	RoleMover    = "mover"
	RoleObserver = "observer"
)

// Results.
const (
	ResultSuccess = "success"
	ResultError   = "error"
)

// Event is one NDJSON record emitted to stdout by a role process.
//
// WorkloadID is always emitted (even 0) because 0 is a valid workload ID;
// lifecycle events without a task use 0 and accounting ignores them by type.
type Event struct {
	Type       string    `json:"type"`
	Role       string    `json:"role"`
	Process    int       `json:"process"`
	WorkloadID uint64    `json:"workload_id"`
	At         time.Time `json:"at"`
	DurationNS int64     `json:"duration_ns,omitempty"`
	Result     string    `json:"result,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// ProcessReady reports that a role process finished startup.
func ProcessReady(role string, process int) Event {
	return Event{
		Type:    TypeProcessReady,
		Role:    role,
		Process: process,
		At:      time.Now(),
	}
}

// EnqueueStarted marks the beginning of one enqueue attempt.
func EnqueueStarted(workloadID uint64, process int) Event {
	return Event{
		Type:       TypeEnqueueStarted,
		Role:       RoleProducer,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
	}
}

// EnqueueFinished records enqueue latency and outcome.
func EnqueueFinished(workloadID uint64, process int, started time.Time, err error) Event {
	event := Event{
		Type:       TypeEnqueueFinished,
		Role:       RoleProducer,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
		DurationNS: time.Since(started).Nanoseconds(),
		Result:     ResultSuccess,
	}
	if err != nil {
		event.Result = ResultError
		event.Error = err.Error()
	}
	return event
}

// HandlerStarted marks the beginning of one handler execution.
func HandlerStarted(workloadID uint64, process int) Event {
	return Event{
		Type:       TypeHandlerStarted,
		Role:       RoleWorker,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
	}
}

// HandlerFinished records execution latency and outcome.
func HandlerFinished(workloadID uint64, process int, started time.Time, err error) Event {
	event := Event{
		Type:       TypeHandlerFinished,
		Role:       RoleWorker,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
		DurationNS: time.Since(started).Nanoseconds(),
		Result:     ResultSuccess,
	}
	if err != nil {
		event.Result = ResultError
		event.Error = err.Error()
	}
	return event
}

// DeadLettered records a terminal DLQ observation independent of workers.
func DeadLettered(workloadID uint64, at time.Time) Event {
	return Event{
		Type:       TypeDeadLettered,
		Role:       RoleObserver,
		Process:    0,
		WorkloadID: workloadID,
		At:         at,
	}
}

// RetryMoved records one retry-to-ready movement.
func RetryMoved(workloadID uint64, process int) Event {
	return Event{
		Type:       TypeRetryMoved,
		Role:       RoleMover,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
	}
}

// ProcessError records a non-fatal process error.
func ProcessError(role string, process int, err error) Event {
	event := Event{
		Type:    TypeProcessError,
		Role:    role,
		Process: process,
		At:      time.Now(),
	}
	if err != nil {
		event.Error = err.Error()
	}
	return event
}

// ProcessStopped records process shutdown.
func ProcessStopped(role string, process int, err error) Event {
	event := Event{
		Type:    TypeProcessStopped,
		Role:    role,
		Process: process,
		At:      time.Now(),
		Result:  ResultSuccess,
	}
	if err != nil {
		event.Result = ResultError
		event.Error = err.Error()
	}
	return event
}

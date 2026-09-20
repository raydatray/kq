package events

import "time"

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

const (
	RoleProducer = "producer"
	RoleWorker   = "worker"
	RoleMover    = "mover"
	RoleObserver = "observer"
)

const (
	ResultSuccess = "success"
	ResultError   = "error"
)

type Event struct {
	Type    string `json:"type"`
	Role    string `json:"role"`
	Process int    `json:"process"`
	// Zero is a valid workload ID; lifecycle events ignore this field by type.
	WorkloadID uint64    `json:"workload_id"`
	At         time.Time `json:"at"`
	DurationNS int64     `json:"duration_ns,omitempty"`
	Result     string    `json:"result,omitempty"`
	Error      string    `json:"error,omitempty"`
}

func ProcessReady(role string, process int) Event {
	return Event{
		Type:    TypeProcessReady,
		Role:    role,
		Process: process,
		At:      time.Now(),
	}
}

func EnqueueStarted(workloadID uint64, process int) Event {
	return Event{
		Type:       TypeEnqueueStarted,
		Role:       RoleProducer,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
	}
}

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

func HandlerStarted(workloadID uint64, process int) Event {
	return Event{
		Type:       TypeHandlerStarted,
		Role:       RoleWorker,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
	}
}

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

func DeadLettered(workloadID uint64, at time.Time) Event {
	return Event{
		Type:       TypeDeadLettered,
		Role:       RoleObserver,
		Process:    0,
		WorkloadID: workloadID,
		At:         at,
	}
}

func RetryMoved(workloadID uint64, process int) Event {
	return Event{
		Type:       TypeRetryMoved,
		Role:       RoleMover,
		Process:    process,
		WorkloadID: workloadID,
		At:         time.Now(),
	}
}

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

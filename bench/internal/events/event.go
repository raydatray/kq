package events

import "time"

type EventType string

const (
	TypeProcessReady    EventType = "process_ready"
	TypeEnqueueStarted  EventType = "enqueue_started"
	TypeEnqueueFinished EventType = "enqueue_finished"
	TypeHandlerStarted  EventType = "handler_started"
	TypeHandlerFinished EventType = "handler_finished"
	TypeDeadLettered    EventType = "dead_lettered"
	TypeRetryMoved      EventType = "retry_moved"
	TypeProcessError    EventType = "process_error"
	TypeProcessStopped  EventType = "process_stopped"
)

type Role string

const (
	RoleProducer Role = "producer"
	RoleWorker   Role = "worker"
	RoleMover    Role = "mover"
	RoleObserver Role = "observer"
)

type Result string

const (
	ResultSuccess Result = "success"
	ResultError   Result = "error"
)

type Event struct {
	Type    EventType `json:"type"`
	Role    Role      `json:"role"`
	Process int       `json:"process"`
	// Zero is a valid workload ID; lifecycle events ignore this field by type.
	WorkloadID uint64    `json:"workload_id"`
	At         time.Time `json:"at"`
	DurationNS int64     `json:"duration_ns,omitempty"`
	Result     Result    `json:"result,omitempty"`
	Error      string    `json:"error,omitempty"`
}

func ProcessReady(role Role, process int) Event {
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

func ProcessError(role Role, process int, err error) Event {
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

func ProcessStopped(role Role, process int, err error) Event {
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

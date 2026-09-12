# Execution Worker

The execution worker acquires tasks from the ready topic through a Kafka share
group and runs application handlers with bounded concurrency.

## Flow

```mermaid
sequenceDiagram
    participant Ready as A-ready
    participant Worker as Execution worker
    participant Handler as Job handler
    participant Retry as Retry topic
    participant DLQ as A-dlq

    Ready->>Worker: Acquire task
    Worker->>Handler: Perform task
    alt Handler succeeds
        Handler-->>Worker: Success
        Worker->>Ready: Acknowledge task as successful
    else Handler fails and attempts remain
        Handler-->>Worker: Retryable error
        Worker->>Retry: Produce task with retry metadata
        Retry-->>Worker: Write confirmed
        Worker->>Ready: Acknowledge original task
    else Failure is permanent or attempts are exhausted
        Handler-->>Worker: Terminal error
        Worker->>DLQ: Produce failed task
        DLQ-->>Worker: Write confirmed
        Worker->>Ready: Acknowledge original task
    else Worker exits before acknowledgement
        Note over Ready: Acquisition expires and the task becomes available again
    end
```

## Bounded Acquisition

The worker only polls for work when its execution pool has capacity. This
prevents it from holding more acquired records than it can execute or safely
renew.

```go
func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		capacity := w.pool.Available()
		if capacity == 0 {
			if err := w.pool.WaitAvailable(ctx); err != nil {
				return err
			}
			continue
		}

		records, err := w.consumer.Poll(ctx, capacity)
		if err != nil {
			return err
		}

		for _, record := range records {
			record := record
			w.pool.Go(func() {
				w.execute(ctx, record)
			})
		}
	}

	return ctx.Err()
}
```

The concrete Kafka consumer should also enforce an acquisition limit so a
single poll cannot exceed the configured worker capacity.

## JSON Job Dispatch

Handlers are registered with factories rather than preconstructed job values.
The factory creates a fresh job and injects its runtime dependencies before KQ
decodes the JSON arguments onto it.

```go
func (m *ServeMux) HandleJob(factory JobFactory) {
	prototype := factory()
	name := prototype.Name()

	if _, exists := m.handlers[name]; exists {
		panic("kq: duplicate job handler for " + name)
	}

	m.handlers[name] = func(ctx context.Context, payload []byte) error {
		job := factory()
		if err := json.Unmarshal(payload, job); err != nil {
			return Permanent(fmt.Errorf("kq: decode job %q: %w", name, err))
		}
		return job.Perform(ctx)
	}
}
```

Creating a new job per task prevents argument data from being shared between
concurrent executions.

## Handler Outcomes

Every acquired record resolves to success, retry, or terminal failure:

```go
func (w *Worker) execute(ctx context.Context, record *shareRecord) {
	envelope, err := decodeEnvelope(record.Value)
	if err != nil {
		w.deadLetterRaw(ctx, record, err)
		return
	}

	err = w.mux.Process(ctx, envelope.Type, envelope.Payload)

	switch {
	case err == nil:
		record.Acknowledge()

	case IsPermanent(err):
		w.moveToDLQ(ctx, record, envelope, err)

	default:
		w.retryOrDeadLetter(ctx, record, envelope, err)
	}
}
```

Handler panics are recovered and treated as retryable failures. Invalid task
envelopes, invalid JSON, and unknown task types are terminal failures because
retrying them cannot repair the record.

## Retrying

The configured delays determine whether another attempt remains. For delays
`[1m, 10m, 1h]`, attempt four is the final execution attempt.

```go
func (w *Worker) retryOrDeadLetter(
	ctx context.Context,
	record *shareRecord,
	envelope taskEnvelope,
	cause error,
) {
	delay, ok := w.queue.retryDelay(envelope.Attempt)
	if !ok {
		w.moveToDLQ(ctx, record, envelope, cause)
		return
	}

	envelope.Attempt++
	envelope.LastError = cause.Error()

	if err := w.writeEnvelope(ctx, w.queue.retryTopic(delay), envelope); err != nil {
		record.Release()
		return
	}

	record.Acknowledge()
}
```

The destination write is confirmed before the ready record is acknowledged.
If the worker exits between those operations, Kafka can retain both records;
this produces a duplicate rather than losing the task.

## Terminal Failure

Permanent and exhausted tasks are moved to the DLQ with the same ordering rule:

```go
func (w *Worker) moveToDLQ(
	ctx context.Context,
	record *shareRecord,
	envelope taskEnvelope,
	cause error,
) {
	envelope.LastError = cause.Error()

	if err := w.writeEnvelope(ctx, w.queue.dlqTopic(), envelope); err != nil {
		record.Release()
		return
	}

	record.Acknowledge()
}
```

KQ never automatically consumes the DLQ. Manual replay creates a new ready
record using the original task identity and payload.

## Shutdown and Recovery

Graceful shutdown stops polling, waits for in-flight handlers for a configured
period, releases unfinished records, flushes acknowledgements, and closes the
Kafka clients.

If a process crashes or loses an acquisition before acknowledgement, Kafka
expires the acquisition and makes the record available to another worker.

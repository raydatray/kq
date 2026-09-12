# kq design requirements

KQ is an embeddable Go job queue built on Kafka share groups. Kafka is the
only required external service.

## Core constraints

- KQ must be a standalone Go library. Producers and workers must be embeddable
  in ordinary Go programs; no KQ daemon is required.
- KQ must use Kafka share groups and require a Kafka version where share groups
  are production-ready.
- Kafka must be the only external system required for queue correctness,
  retries, and recovery.
- All durable KQ state must be stored in Kafka. Process-local state may only be
  used as a cache or while work is actively executing.
- The public API should expose job-queue concepts rather than Kafka client
  records and acknowledgement types.

## Task model and API

- Every task must have a stable unique ID, a task type, a payload, and enqueue
  metadata.
- The core payload must remain opaque bytes.
- KQ must provide a typed JSON job API while preserving access to the raw task
  API.
- A typed job must be able to contain serialized arguments while receiving
  non-serialized runtime dependencies from a worker-side factory.
- A queue may contain multiple task types. Workers must be able to dispatch
  each type to its registered handler.
- Enqueue must return only after Kafka has durably acknowledged the record, or
  return an error stating that durability is unknown or was not achieved.
- Producers must be safe for concurrent use.

## Delivery semantics

- Delivery is at least once. A handler may execute more than once for the same
  task, including after it has completed successfully.
- KQ must acknowledge a task as successful only after its handler returns
  successfully.
- If a worker exits or loses its acquisition without acknowledging a task, the
  task must become eligible for redelivery after its acquisition expires.
- An acknowledgement failure or shutdown during execution must leave the task
  eligible for redelivery.
- KQ must expose the task ID and current attempt or delivery count to handlers.
- KQ does not guarantee task execution order, including between tasks in the
  same queue.
- KQ does not provide exactly-once execution. Applications must make handlers
  idempotent when duplicate side effects are unacceptable.
- Delivery guarantees are subject to Kafka's configured durability, retention,
  and administrative deletion policies. KQ must document the required broker
  and topic settings.

## Retries and terminal failure

- Handler errors and panics must be retryable by default.
- KQ must support a finite maximum attempt count. The attempt count includes
  the initial execution.
- KQ must support configurable fixed retry delays.
- Retry state and delayed retries must survive producer and worker restarts and
  must not require storage outside Kafka.
- Handlers must be able to mark an error as permanent so the task is not
  retried.
- Invalid envelopes, undecodable payloads, and unknown task types must not
  enter an infinite or hot retry loop.
- Tasks that exhaust retries or fail permanently must be retained in a
  dead-letter queue with their original identity, type, payload, attempt count,
  and failure information.
- Dead-lettered tasks should be inspectable and replayable.

## Worker behavior

- Worker concurrency must be configurable and strictly bounded.
- Workers must apply backpressure and must not acquire substantially more work
  than they have capacity to execute or safely renew.
- Multiple worker processes must be able to consume the same queue and share
  group for horizontal scaling, including when a topic has few partitions.
- A handler must receive a context that is canceled on task timeout, worker
  shutdown, or acquisition loss where detectable.
- KQ must support default and per-task execution timeouts or deadlines.
- Worker panics must not crash the process or cause the task to be acknowledged
  as successful.
- Graceful shutdown must stop acquiring new tasks, allow in-flight handlers a
  configurable drain period, release unfinished tasks, flush acknowledgements,
  and close Kafka clients.
- Handler middleware should be supported for logging, tracing, metrics,
  recovery, and application concerns.

## Queues

- KQ must support multiple named queues so applications can isolate workloads
  with different concurrency, scaling, and failure characteristics.
- Related task types may share a queue; applications should use separate queues
  when they require resource or failure isolation.

## Logging and metrics

- KQ must provide built-in structured logging using `log/slog`.
- KQ must provide built-in Prometheus metrics for enqueue results, queue delay,
  execution duration, in-flight tasks, retries, successes, terminal failures,
  retry movement, and acknowledgement failures.
- Task payloads must not be logged, and task IDs must not be used as metric
  labels.
- Applications must be able to supply the logger and Prometheus registerer.

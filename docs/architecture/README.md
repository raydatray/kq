# kq architecture

KQ is an embeddable Go job queue built on Kafka share groups. Producers write
tasks to a ready topic, execution workers perform them, and retry movers return
failed tasks after a count-based delay mapped onto a Kafka delay grid.

Topic names below are illustrative for a queue named `A`.

## Topology

```mermaid
flowchart LR
    App[Application] --> Producer[KQ producer]
    Producer --> Ready[(A-ready<br/>share-group topic)]

    Ready --> Workers[Execution workers<br/>bounded concurrency]
    Workers -->|success| Done((Done))
    Workers -->|retryable failure| Retry[(A-retry-*<br/>delay bands and partition ranges)]
    Workers -->|permanent failure or attempts exhausted| DLQ[(A-dlq)]

    Retry --> Movers[Retry movers<br/>classic consumer group]
    Movers -->|delay elapsed| Ready
```

Execution workers and retry movers are independent runners. A process may run
either one or both, allowing retry movement to be embedded in worker pods or
run as a dedicated deployment. All movers for a queue use the same classic
consumer group.

## Task Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Ready: Enqueued
    Ready --> Running: Acquired
    Running --> Completed: Handler succeeds and task is acknowledged
    Running --> Ready: Worker exits or releases task
    Running --> WaitingToRetry: Retryable failure
    WaitingToRetry --> Ready: Retry delay elapses
    Running --> DeadLettered: Permanent failure
    Running --> DeadLettered: Attempts exhausted
    DeadLettered --> Ready: Manual replay
    Completed --> [*]
```

Moving a task creates a new Kafka record while preserving its task ID. KQ
confirms the destination write before acknowledging or committing the source,
which prevents task loss but permits duplicate records under failure.

## Components

- [Producer](producer.md)
- [Execution worker](worker.md)
- [Retry mover](retry-mover.md)
- [Scaling](../scaling.md)
- [Proposed public API](../api.md)
- [Design requirements](../requirements.md)

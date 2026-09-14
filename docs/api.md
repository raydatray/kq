# Proposed kq API

This document describes the intended public API. It is a design target, not a
compatibility commitment.

## Queue Configuration

A queue configuration defines the logical queue and its retry policy:

```go
queue := kq.QueueConfig{
	Name: "email",
	RetryDelays: []time.Duration{
		time.Minute,
		10 * time.Minute,
		time.Hour,
	},
}
```

The initial execution counts as attempt one. Three retry delays therefore
allow at most four intentionally scheduled attempts:

```text
attempt 1 fails -> wait 1 minute
attempt 2 fails -> wait 10 minutes
attempt 3 fails -> wait 1 hour
attempt 4 fails -> dead-letter
```

The delays above are the logical retry policy. Physical retry topics are delay
bands whose partitions represent bounded delay ranges. KQ stores each requested
delay exactly and routes it to the containing range, as described in the
[retry mover design](architecture/retry-mover.md).

An illustrative delay grid may contain topics such as:

```text
email-ready
email-retry-0s
email-retry-2m
email-retry-8m
email-dlq
```

The exact retry-policy API and delay-grid configuration remain design targets.
The execution worker and retry mover must use the same resolved grid.

## JSON Jobs

A job defines its stable name and execution behavior:

```go
type Job interface {
	Name() string
	Perform(context.Context) error
}

type JobFactory func() Job
```

An application job contains exported JSON arguments and unexported runtime
dependencies:

```go
package emailjobs

type Mailer interface {
	Send(context.Context, string, string) error
}

type SendEmailJob struct {
	To       string `json:"to"`
	Template string `json:"template"`

	mailer Mailer
}

func (*SendEmailJob) Name() string {
	return "email:send"
}

func (j *SendEmailJob) Perform(ctx context.Context) error {
	if j.To == "" {
		return kq.Permanent(errors.New("recipient is required"))
	}

	return j.mailer.Send(ctx, j.To, j.Template)
}
```

The producer constructor populates only serialized arguments:

```go
func NewSendEmail(to, template string) *SendEmailJob {
	return &SendEmailJob{
		To:       to,
		Template: template,
	}
}
```

The worker factory injects the service dependency:

```go
func SendEmailFactory(mailer Mailer) kq.JobFactory {
	return func() kq.Job {
		return &SendEmailJob{
			mailer: mailer,
		}
	}
}
```

When a task executes, KQ first calls the factory:

```go
job := factory()

// &SendEmailJob{
//     To:       "",
//     Template: "",
//     mailer:   mailer,
// }
```

KQ then decodes the stored JSON onto that value:

```go
if err := json.Unmarshal(payload, job); err != nil {
	return kq.Permanent(err)
}

// &SendEmailJob{
//     To:       "ray@example.com",
//     Template: "welcome",
//     mailer:   mailer,
// }
```

JSON populates the exported arguments without changing the unexported
`mailer`. KQ then calls `job.Perform(ctx)`. A fresh job is created for every
task execution.

## Enqueueing Jobs

Create a client for the queue:

```go
client, err := kq.NewClient(kq.ClientConfig{
	Brokers: []string{"localhost:9092"},
	Queue:   queue,
})
if err != nil {
	return err
}
defer client.Close()
```

Enqueue a typed JSON job:

```go
info, err := client.Enqueue(ctx, emailjobs.NewSendEmail(
	"ray@example.com",
	"welcome",
))
if err != nil {
	return err
}

log.Printf("enqueued task %s", info.ID)
```

`Enqueue` JSON-encodes the job, assigns a task ID, writes it to
`email-ready`, and returns only after Kafka confirms the write.

```go
type TaskInfo struct {
	ID string
}
```

## Raw Tasks

Applications that do not want the JSON job API can enqueue opaque bytes:

```go
task := kq.NewTask("image:resize", protobufPayload)

info, err := client.EnqueueTask(ctx, task)
```

The raw task API controls payload encoding but otherwise uses the same task
identity, retry, and delivery behavior.

## Registering Jobs

Register factories before starting the execution worker:

```go
mux := kq.NewServeMux()

mux.HandleJob(emailjobs.SendEmailFactory(mailer))
mux.HandleJob(imagejobs.ResizeFactory(imageProcessor))
```

`HandleJob` calls each factory to discover its job name and rejects duplicate
registrations. When a task arrives, it creates a fresh job, decodes its JSON
arguments, and invokes `Perform`.

Unknown task names and invalid JSON are permanent failures and are moved to the
DLQ rather than retried indefinitely.

The raw task API may register an explicit handler:

```go
mux.HandleTask("image:resize", func(ctx context.Context, task *kq.Task) error {
	return processProtobuf(ctx, task.Payload())
})
```

## Running an Execution Worker

Create a bounded execution worker using the registered handlers:

```go
worker, err := kq.NewWorker(kq.WorkerConfig{
	Brokers:         []string{"localhost:9092"},
	Queue:           queue,
	Concurrency:     20,
	ShutdownTimeout: 30 * time.Second,
}, mux)
if err != nil {
	return err
}
```

Run until the context is canceled or the worker encounters a fatal error:

```go
if err := worker.Run(ctx); err != nil {
	return err
}
```

The worker stops acquiring tasks during shutdown, waits up to
`ShutdownTimeout` for handlers, releases unfinished tasks, and flushes Kafka
acknowledgements.

## Retry and Permanent Failure

A normal handler error schedules the next configured retry:

```go
func (j *SendEmailJob) Perform(ctx context.Context) error {
	return j.mailer.Send(ctx, j.To, j.Template)
}
```

A permanent error bypasses the remaining retries:

```go
func (j *SendEmailJob) Perform(ctx context.Context) error {
	if !validAddress(j.To) {
		return kq.Permanent(errors.New("invalid recipient"))
	}

	return j.mailer.Send(ctx, j.To, j.Template)
}
```

```go
func Permanent(err error) error
func IsPermanent(err error) bool
```

Handler panics are recovered and treated as retryable errors. A task is moved
to the DLQ after a permanent error or after all configured attempts fail.

## Running a Retry Mover

Create a mover from the same queue configuration:

```go
mover, err := kq.NewRetryMover(kq.RetryMoverConfig{
	Brokers: []string{"localhost:9092"},
	Queue:   queue,
})
if err != nil {
	return err
}
```

Run it independently:

```go
if err := mover.Run(ctx); err != nil {
	return err
}
```

Every mover for the same queue joins the same internally derived classic
consumer group. This allows embedded and dedicated movers to coexist without
moving each retry record once per deployment.

## Deployment Modes

Execution and retry movement expose a common lifecycle:

```go
type Runner interface {
	Run(context.Context) error
}
```

Run both in a normal worker process:

```go
return kq.Run(ctx, worker, mover)
```

Run only execution workers:

```go
return worker.Run(ctx)
```

Run a dedicated retry deployment:

```go
return mover.Run(ctx)
```

If any runner returns a fatal error, `kq.Run` cancels the shared context and
waits for the other runners to shut down.

## Logging and Metrics

KQ has built-in structured logging through `log/slog`. Components accept a
logger and use `slog.Default()` when none is supplied:

```go
worker, err := kq.NewWorker(kq.WorkerConfig{
	Brokers:     []string{"localhost:9092"},
	Queue:       queue,
	Concurrency: 20,
	Logger:      slog.Default(),
}, mux)
```

Logs include the queue, task ID, task type, attempt, retry delay, and error when
those values are relevant. Task payloads are not logged.

```go
logger.WarnContext(ctx, "task scheduled for retry",
	"queue", queue.Name,
	"task_id", task.ID,
	"task_type", task.Type,
	"attempt", task.Attempt,
	"retry_delay", delay,
	"error", err,
)
```

KQ also provides built-in Prometheus metrics. A metrics instance is registered
once and shared by every KQ component in the process:

```go
metrics, err := kq.NewPrometheusMetrics(prometheus.DefaultRegisterer)
if err != nil {
	return err
}

client, err := kq.NewClient(kq.ClientConfig{
	Brokers: []string{"localhost:9092"},
	Queue:   queue,
	Metrics: metrics,
})
if err != nil {
	return err
}

worker, err := kq.NewWorker(kq.WorkerConfig{
	Brokers:     []string{"localhost:9092"},
	Queue:       queue,
	Concurrency: 20,
	Metrics:     metrics,
}, mux)
if err != nil {
	return err
}

mover, err := kq.NewRetryMover(kq.RetryMoverConfig{
	Brokers: []string{"localhost:9092"},
	Queue:   queue,
	Metrics: metrics,
})
if err != nil {
	return err
}
```

Initial metrics cover enqueue results, in-flight tasks, execution duration,
task outcomes, retry movement, dead-lettering, and acknowledgement failures.
Task IDs are never used as metric labels because they have unbounded
cardinality.

Logging is enabled by default. Prometheus collection is disabled when no
metrics instance is supplied.

# 0003: Worker Concurrency Recap

Plan: [0003: Worker Concurrency](../plans/0003-worker-concurrency.md)

## Outcome

Workers now execute bounded poll generations through a fixed goroutine pool.
`WorkerConfig.Concurrency` is a required positive value. `Run` starts exactly
that many long-lived pool goroutines once, and the calling goroutine remains the
single Kafka poll and acknowledgement coordinator. No goroutine is created per
task.

```text
poll up to X records
-> feed X persistent workers through a bounded channel
-> resolve records as handlers finish
-> flush all acknowledgements
-> poll the next generation
```

Successful handlers and confirmed retry or DLQ transitions accept their source
records. Decode, transition, destination-write, and canceled-handler failures
release their records. Retry and DLQ writes still complete before source
acceptance. Acknowledgement callback errors are surfaced after flush rather
than being silently discarded.

The performance harness records `topology.worker_concurrency`; its CI smoke
profile uses concurrency 2 and its local baseline profile retains concurrency 1.

## Verification

The stack passed:

```text
go vet ./...
go test ./...
go test -race ./...
sh test/integration/run.sh
GOFLAGS=-race sh test/integration/run.sh
uv run --project bench python -m unittest discover \
  -s bench/kqbench -t bench -p 'test_*.py' -v
```

Unit coverage verifies the concurrency bound, actual overlap, out-of-order task
completion, one terminal decision per record, flush-before-next-poll behavior,
joined task and acknowledgement failures, and cancellation draining. Kafka
integration coverage preloads more work than the pool, observes four distinct
handlers active together, and accounts for every task.

## Published Runs

- [Worker concurrency sweep](../performance/runs/2026-09-21-ready-success-v1-worker-concurrency-sweep-00bb923/analysis.md)

The controlled comparison used one revision, two producers, two worker
processes, one retry mover, one ready partition, and a lognormal execution
distribution with a 5 ms average and 20 ms p99. It varied only requested RPS
and per-process worker concurrency.

## Observations

| Concurrency | Highest sustainable tested RPS | 800 RPS worker rate | 800 RPS queue p95 |
| ---: | ---: | ---: | ---: |
| 1 | 300 | 285.5/s | 17.29 s |
| 2 | 400 | 458.6/s | 7.09 s |
| 4 | 400 | 756.1/s | 547 ms |
| 8 | 800 | 801.2/s | 10.9 ms |

Concurrency 8 sustained the highest tested rate of 800 RPS with an 11 ms drain.
At that point it delivered 2.81 times the observed worker throughput of
concurrency 1 and reduced queue p95 by 99.94%. All 81,000 measured tasks were
completed with zero duplicates, missing, outstanding, or dead-lettered IDs.

## Next Step

The simple fixed pool solves the measured serial bottleneck. Do not move
directly to explicit-ack streaming based on this short-tail workload. First run
a heavy-tail scenario that exposes generation head-of-line blocking, then use
that evidence to decide whether continuous refill is worth the additional
acknowledgement and renewal complexity.

Acquisition renewal is independently required before supporting handlers that
may approach Kafka's share-record lock duration. The performance harness should
also add worker-process CPU and memory telemetry before drawing efficiency
conclusions from concurrency comparisons.

## Limitations

```text
fixed generations: the next poll waits for the slowest task in the generation
shutdown: handlers must honor context cancellation; no separate timeout exists
acquisition locks: long-running handlers are not renewed
resource metrics: worker CPU and memory are not collected
benchmark: each point ran once on a local arm64 laptop
```

# Performance Scenarios

Scenarios are stable, versioned workload definitions. Local runs, CI smoke
tests, controlled benchmarks, and published runs use the same scenario IDs.

Catalogue scenarios are preferred but not mandatory. A one-off investigation
may provide an ad hoc configuration directly to the harness. Its `run.json`
records `scenario` as `null` and preserves the fully resolved workload. Reused
ad hoc workloads should become versioned catalogue scenarios.

Profiles may change task count, target rate, duration, and process counts, but
must not change the behavior or correctness question defined by a scenario.

| Scenario ID | Behavior | Primary question |
| --- | --- | --- |
| `ready-success-v1` | Every task succeeds. | What throughput and latency can the ready path sustain? |
| `execution-latency-v1` | Tasks sample a configured execution-time distribution. | How does handler cost affect saturation? |
| `permanent-failure-v1` | A deterministic percentage always fails and reaches the DLQ. | Are terminal tasks accounted for under load? |
| `temporary-outage-v1` | A deterministic percentage fails until a shared timestamp. | How does the system recover after an outage? |
| `retry-herd-v1` | Many retries become eligible within the same window. | How quickly and smoothly is retry backlog drained? |

Process scaling uses an existing scenario, usually `ready-success-v1`, with
profile or command-line topology overrides. It does not require a separate
scenario because the workload behavior is unchanged.

Run one scenario with different profiles:

```bash
uv run --project bench kq-bench run \
  --scenario ready-success-v1 \
  --profile ci-smoke

uv run --project bench kq-bench run \
  --scenario ready-success-v1 \
  --profile benchmark
```

Changing scenario behavior requires a new versioned ID. Existing scenario
definitions and published run metadata remain immutable.

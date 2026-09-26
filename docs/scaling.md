# Scaling

This document collects the limits, relations, and measured behavior that
determine how far a KQ queue scales. Use it to size a queue: worker processes,
shards, shard concurrency, ready partitions, and broker settings.

Every rule is labeled with its evidence:

| Label | Meaning |
| --- | --- |
| **Kafka limit** | Broker configuration default or hard bound, observed on `apache/kafka:4.3.1`. |
| **Code** | Follows directly from KQ's implementation. |
| **Derived** | Follows from other rules; not measured on its own. |
| **Measured** | Observed in a published run (linked). |
| **Hypothesis** | Fits the measurements but has not been confirmed directly. |

All measurements so far come from one arm64 laptop running a single Kafka
broker with RF=1 and sleep-only handlers. Treat measured values as starting
points until they are repeated on a multi-broker cluster.

## Symbols

| Symbol | Meaning |
| --- | --- |
| `λ` | Task arrival rate, tasks/s |
| `E` | Handler execution time; `E_avg` is its mean |
| `C` | Shard concurrency: records per generation and handlers per shard |
| `M` | Share-group members for the queue |
| `S` | Handler slots, `M x C` |
| `G(C)` | Generation duration for a shard of concurrency `C` |
| `P` | Ready topic partitions |
| `K` | Record locks per share partition |
| `T_ack` | Time from acquiring a partition's oldest in-flight record to acknowledging it |
| `ρ` | Slot utilization, `λ / slot capacity` |

Until [0005](plans/0005-internal-consumer-sharding.md) lands, each `kq.Worker`
is one member with `C = Concurrency`. With 0005, each shard is one member with
`C = ShardConcurrency`.

## Kafka Limits

| Setting | Default | Bound | Why it matters |
| --- | ---: | ---: | --- |
| `group.share.max.size` | 200 | max 1000 | Caps members per share group, across every worker process. |
| `group.share.partition.max.record.locks` | 2000 | max 4000 (`group.share.max.partition.max.record.locks`), min 100 | Caps in-flight records per partition. |
| `group.share.record.lock.duration.ms` | 30,000 | 15,000 to 60,000 | A record not acknowledged within this time is redelivered. |
| `group.share.delivery.count.limit` | 5 | 2 to 10 | A record delivered this many times without acceptance is archived by Kafka. |
| `group.share.max.share.sessions` | 2000 | | Share sessions per broker; each member holds one per broker it fetches from. |
| `group.share.assignment.interval.ms` | 1000 | | Minimum interval between partition reassignments. |

- **Kafka limit.** `group.share.max.size=1001` is rejected at broker startup
  ("Value must be no more than 1000"). The runs set it as a static broker
  setting. Whether a per-group override is supported was not verified.
- **Kafka limit.** The bench and integration compose files lower the minimum
  lock duration to 1000 ms. Production brokers use the defaults above unless
  configured otherwise.
- **Hypothesis.** `group.share.max.share.sessions` has not been reached. With
  1000 members on a multi-broker cluster, each member may hold a session on
  every broker it fetches from. Check this limit before assuming 1000 members
  work on every cluster size.

## Relations

### Members and Slots

```text
M = Σ shards across every worker process    ≤ group.share.max.size ≤ 1000
S = M x C
```

**Kafka limit / Code.** Partition count does not raise `M`. Every member of a
share group may read every partition.

### Tasks In Flight

```text
L = λ x E_avg                                (Little's law)
```

**Derived.** At 20,000 tasks/s with `E_avg = 250 ms`, about 5,000 tasks are
executing at any moment. `S` must be well above `L` because of the generation
wait below.

### Generation Duration

A shard polls up to `C` records, waits for every one, flushes acknowledgements,
and only then polls again (**Code**, `worker.go`). A generation therefore lasts
about as long as its slowest record:

```text
G(C) ≈ E[max of C execution samples] + overhead
```

For the north-star lognormal (250 ms average, 646 ms p99):

| `C` | `E[max]` | Slot utilization `E_avg / G` |
| ---: | ---: | ---: |
| 1 | 250 ms | 100% |
| 10 | 468 ms | 53% |
| 20 | 539 ms | 46% |
| 40 | 615 ms | 41% |
| 100 | 715 ms | 35% |
| 1000 | 980 ms | 26% |

- **Derived.** `E[max]` is computed by sampling the lognormal.
- **Measured.** Fitted overhead is about 10 to 15 ms locally.

### Slot Capacity

```text
λ_slots ≈ M x C / G(C)
```

**Measured.** With `M = 1000`:

| `C` | Predicted `λ_slots` | Observed |
| ---: | ---: | --- |
| 10 | about 20,800/s | Plateaued at 20,900/s ([run 5](performance/runs/2026-09-23-adhoc-shard-10-ramp-33c95bf/analysis.md)) |
| 20 | about 36,300/s | Sustained 36,000/s; ceiling not reached ([run 6](performance/runs/2026-09-23-adhoc-shard-20-ramp-33c95bf/analysis.md)) |

Raising `C` raises capacity sublinearly because `G` grows with `C`.

### Partition Capacity

A share partition's in-flight window starts at its oldest unacknowledged record
and holds at most `K` records. A slow record at the front stops the window from
advancing:

```text
λ_partitions ≈ P x K / T_ack
```

- **Hypothesis.** Not confirmed from broker metrics.
- **Measured.** Fitted `T_ack` is about 1.0 to 1.15 s on the north-star
  workload, larger than `G`. Predictions: 2 partitions about 7,300/s and 4
  partitions about 16,000/s, against observed collapse peaks of 7,274 and
  16,123/s
  ([run 4](performance/runs/2026-09-23-adhoc-share-group-1000-members-33c95bf/analysis.md)).
  8 partitions at `C = 20`: about 28,000 to 32,000/s predicted, 26,000 to
  28,000/s observed limit (run 6).

### Throughput Ceiling

```text
λ_max ≈ min(λ_slots, λ_partitions, broker capacity, worker host capacity)
```

**Derived.** On the local host, broker and host capacity bound first near
36,000 to 40,000 tasks/s. See [Resources](#resources).

### Queue Time

Queue time is the wait from enqueue to handler start. It is mostly the time a
record waits for a member to finish its current generation.

- **Measured.** With zero execution time, queue p99 was 6 to 9 ms
  ([run 3](performance/runs/2026-09-23-adhoc-queue-latency-diagnosis-33c95bf/analysis.md)).
  That is the floor Kafka and the worker loop add.
- **Measured.** Queue time falls as spare capacity rises, even though a larger
  `C` makes each generation longer:

| Topology | `ρ` | Queue p50 / p95 / p99 |
| --- | ---: | ---: |
| 1000 x `C = 10`, 20,000/s, 8 partitions | 0.96 | 14 / 48 / 99 ms |
| 1000 x `C = 20`, 20,000/s, 8 partitions | 0.55 | 4 / 13 / 22 ms |
| 1000 x `C = 20`, 34,000/s, 16 partitions | 0.94 | 22 / 66 / 95 ms |
| 32 x `C = 1000`, 20,000/s, 32 partitions | 0.62 | 465 / 965 / 1,187 ms |

- **Measured.** Very large `C` has high queue time at any utilization
  ([run 2](performance/runs/2026-09-23-adhoc-lean-probe-capacity-33c95bf/analysis.md)).
  Each member takes a long generation to return for more work.
- **Measured.** Queue time rises with surplus partitions. At 1000 x `C = 10`
  and 20,000/s, p50 was 14, 24, 42, and 73 ms at 8, 16, 32, and 64 partitions
  (run 5). The cause is unconfirmed.

### Lock Duration

```text
E_max + generation wait + acknowledgement  <  group.share.record.lock.duration.ms
```

**Code.** KQ does not renew acquisitions. A record's lock covers its own
execution and the rest of its generation. The longest handler must finish
comfortably inside the lock duration, or the record is redelivered while still
running. Each redelivery counts toward `group.share.delivery.count.limit`.
Beyond that limit Kafka archives the record, and KQ never sees it again.

### Producers

```text
λ_enqueue per client ≈ concurrent Enqueue callers / enqueue latency
```

- **Code.** `Enqueue` is a synchronous `acks=all` produce.
- **Measured.** Serial callers achieved about 1,500 to 3,000 enqueues/s each
  (0.3 to 0.7 ms per call). One client shared by 512 goroutines reached
  40,000/s, and 4 such clients reached 80,000/s (run 2). franz-go batches
  concurrent calls, so share one `kq.Client` across goroutines rather than
  enqueueing serially.

## Failure Modes

| Condition | Behavior | Evidence |
| --- | --- | --- |
| `λ > λ_partitions` | Throughput collapses instead of plateauing: completions peak, then decay, while broker CPU rises. | **Measured** (run 4). Cause is a hypothesis. |
| `λ > λ_slots` | Throughput plateaus at `λ_slots`; queue time grows without bound. | **Measured** (run 5) |
| `ρ` above about 0.95 | Queue p95 and p99 rise sharply before throughput plateaus. | **Measured** (runs 5, 6) |
| Members > `group.share.max.size` | Extra members cannot join. | **Kafka limit** |
| Handler exceeds lock duration | Record redelivered while running; archived after the delivery limit. | **Code / Kafka limit**; not measured |
| Many members join or leave together | Assignments change repeatedly until membership settles. Duration unmeasured; runs waited 40 s at 1000 members. | **Hypothesis** |

## Sizing Procedure

1. Estimate `λ_peak` and the execution distribution.
2. Choose `C` so that `M x C / G(C)` gives headroom with `M ≤ 1000`. Target
   `ρ ≤ 0.9` at `λ_peak` for Redis-level queue time.
3. Choose the smallest `P` with `P x K / T_ack ≥ λ_peak / 0.7`, using
   `K = 4000` and `T_ack ≈ 1.1 s` until measured for your workload.
4. Set `group.share.max.size ≥ M` and `group.share.partition.max.record.locks = K`.
5. Spread `M` across worker processes. 100 members per process is the only
   tested shape.
6. Confirm the longest handler is well inside the lock duration.
7. Verify with a ramp before relying on the result.

## Worked Example

North-star workload: 250 ms average, 646 ms p99, target 34,000 tasks/s.

```text
C = 20:     G ≈ 539 + 12 ms ≈ 0.55 s
            λ_slots ≈ 1000 x 20 / 0.55 ≈ 36,300/s
            ρ at 34,000/s ≈ 0.94            (above the 0.9 target)
P:          34,000 / 0.7 x 1.1 / 4000 ≈ 13.4 → 16 partitions
members:    10 processes x 100 shards = 1000
```

**Measured.** This configuration sustained 34,000/s at 16 partitions with
22 / 66 / 95 ms queue p50 / p95 / p99 (run 6). At `ρ = 0.94` it is close to
its limit. 36,000/s was sustained but exceeded the p95 target. For headroom at
34,000/s, raise `C` or split the queue.

At 20,000/s with the same topology, `ρ ≈ 0.55`, and step 3 gives 8
partitions. Run 6 measured 4 / 13 / 22 ms there.

## Resources

**Measured**, single broker, local host:

| Component | Cost |
| --- | --- |
| Kafka broker | About 3.4 to 3.7 cores for enqueue alone from 12,000 to 80,000/s, and 4.7 to 5.5 cores with workers consuming; about 6 cores with 1000 members at 20,000 to 36,000 tasks/s. |
| Workers | About 0.7 cores per 20,000 tasks/s with 32 members; about 1.6 to 1.9 cores with 1000 members. Sleep-only handlers. |
| Producers | About 1.5 to 2.5 cores at 20,000 to 120,000 enqueues/s with shared clients. |

Real handlers consume their own CPU, memory, and downstream connections.
Measure worker resources with the production handler.

## Not Yet Characterized

- Retry movers, retry topics, and the DLQ at scale. The DLQ is written only to
  partition 0.
- Multi-broker clusters, RF=3, and cross-zone latency.
- Membership changes: startup convergence and rolling restarts.
- Continuous refill, which would change `G(C)` and invalidate the slot capacity
  relation above.

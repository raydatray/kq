"""Throughput and latency aggregation over explicit measurement windows.

Every rate is bound to the window it was measured over so cold-start and
shutdown periods cannot silently dilute steady-state behavior:

```text
enqueue window: first enqueue_started -> last enqueue_finished
active window:  first handler_started -> last handler_finished
drain window:   last enqueue_finished -> last terminal outcome
startup:        earliest worker process_ready -> first handler_started
total:          Kafka start -> cleanup finished (duration_ms)
```

Warm-up IDs are excluded from every window and latency except startup,
where the first execution of any ID proves share-group usability.
"""

from __future__ import annotations

import datetime
import math
from typing import Any

from .accounting import Correctness
from .events import Event, parse_events


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0
    index = math.ceil(quantile * len(ordered)) - 1
    return ordered[max(index, 0)]


def summarize(
    config: dict[str, Any],
    raw_events: list[Any],
    correctness: Correctness,
    *,
    total_duration_ms: int,
    warmup_ids: set[int] | frozenset[int] | None = None,
) -> dict[str, Any]:
    events: list[Event]
    if raw_events and isinstance(raw_events[0], dict):
        events = parse_events(raw_events)  # type: ignore[arg-type]
    else:
        events = list(raw_events)  # type: ignore[arg-type]

    excluded = warmup_ids or frozenset()
    measured = [
        event
        for event in events
        if event.workload_id is None or event.workload_id not in excluded
    ]

    requested = int(config["workload"]["arrival"]["target_rps"])
    retry_delay_ms = float(config["kq"].get("retry_delay_ms", 0))
    total_s = max(float(total_duration_ms) / 1000, 1e-3)

    enqueue_span = _enqueue_span(measured)
    achieved = _rate(correctness.produced, enqueue_span, total_s)

    active_span = _active_span(measured)
    active_worker = _rate(correctness.completed, active_span, total_s)

    drain_span, drain_rate = _drain(measured, total_s)

    startup_ms = _startup_ms(events)

    handler_errors = sum(
        1 for e in measured if e.type == "handler_finished" and e.result != "success"
    )
    # Derived mover throughput: each retry schedules one error plus one move,
    # while each DLQ task contributes a final error with no move.
    retry_moves = max(0, handler_errors - correctness.dead_lettered)
    moves_rate = _rate(retry_moves, active_span, total_s)
    dlq_rate = _rate(correctness.dead_lettered, active_span, total_s)

    enqueue_ok = [e for e in measured if e.type == "enqueue_finished" and e.result == "success"]
    enqueue_latencies = [e.duration_ns / 1e6 for e in enqueue_ok if e.duration_ns > 0]

    enqueue_at: dict[int, datetime.datetime] = {}
    for event in enqueue_ok:
        if event.workload_id is not None and event.at is not None:
            enqueue_at.setdefault(event.workload_id, event.at)

    first_started_at: dict[int, datetime.datetime] = {}
    for event in measured:
        if event.type == "handler_started" and event.workload_id is not None and event.at is not None:
            first_started_at.setdefault(event.workload_id, event.at)

    terminal_at: dict[int, datetime.datetime] = {}
    for event in measured:
        if event.workload_id is None or event.at is None:
            continue
        if event.type == "handler_finished" and event.result == "success":
            terminal_at.setdefault(event.workload_id, event.at)
        elif event.type == "dead_lettered":
            terminal_at.setdefault(event.workload_id, event.at)

    queue: list[float] = []
    for workload_id, enqueued in enqueue_at.items():
        started = first_started_at.get(workload_id)
        if started is not None:
            queue.append(max(0.0, (started - enqueued).total_seconds() * 1000))

    deliveries: list[float] = []
    for workload_id, finished in terminal_at.items():
        enqueued = enqueue_at.get(workload_id)
        if enqueued is not None:
            deliveries.append(max(0.0, (finished - enqueued).total_seconds() * 1000))

    executions = [
        e.duration_ns / 1e6
        for e in measured
        if e.type == "handler_finished" and e.duration_ns > 0
    ]

    retry_lags = compute_retry_lags(measured, retry_delay_ms)

    execution_avg = sum(executions) / len(executions) if executions else 0

    return {
        "duration_ms": int(total_duration_ms),
        "throughput": {
            "requested_enqueue_per_second": requested,
            "achieved_enqueue_per_second": achieved,
            "active_worker_per_second": active_worker,
            "drain_per_second": drain_rate,
            "retry_moves_per_second": moves_rate,
            "dead_lettered_per_second": dlq_rate,
        },
        "windows_ms": {
            "enqueue": enqueue_span * 1000,
            "active": active_span * 1000,
            "drain": drain_span * 1000,
            "startup": startup_ms,
        },
        "latency_ms": {
            "enqueue_p50": percentile(enqueue_latencies, 0.50),
            "enqueue_p95": percentile(enqueue_latencies, 0.95),
            "enqueue_p99": percentile(enqueue_latencies, 0.99),
            "queue_p50": percentile(queue, 0.50),
            "queue_p95": percentile(queue, 0.95),
            "queue_p99": percentile(queue, 0.99),
            "delivery_p50": percentile(deliveries, 0.50),
            "delivery_p95": percentile(deliveries, 0.95),
            "delivery_p99": percentile(deliveries, 0.99),
            "execution_average": execution_avg,
            "execution_p95": percentile(executions, 0.95),
            "execution_p99": percentile(executions, 0.99),
            "execution_max": max(executions, default=0),
            "retry_lag_p50": percentile(retry_lags, 0.50),
            "retry_lag_p95": percentile(retry_lags, 0.95),
            "retry_lag_p99": percentile(retry_lags, 0.99),
        },
        "correctness": {
            "produced": correctness.produced,
            "completed": correctness.completed,
            "dead_lettered": correctness.dead_lettered,
            "outstanding": len(correctness.outstanding),
            "duplicates": correctness.duplicates,
            "missing": len(correctness.missing),
        },
    }


def _rate(count: float, span: float, total_s: float) -> float:
    if span > 0:
        return count / span
    # Degenerate single-event windows fall back to the total run duration.
    return count / total_s if count else 0.0


def _enqueue_span(measured: list[Event]) -> float:
    starts = sorted(
        e.at for e in measured if e.type == "enqueue_started" and e.at is not None
    )
    finishes = sorted(
        e.at for e in measured if e.type == "enqueue_finished" and e.at is not None
    )
    if starts and finishes:
        return max(0.0, (finishes[-1] - starts[0]).total_seconds())
    if len(finishes) >= 2:
        # Logs predating enqueue_started fall back to the finish span.
        return max(0.0, (finishes[-1] - finishes[0]).total_seconds())
    return 0.0


def _active_span(measured: list[Event]) -> float:
    first_start = min(
        (e.at for e in measured if e.type == "handler_started" and e.at is not None),
        default=None,
    )
    last_finish = max(
        (e.at for e in measured if e.type == "handler_finished" and e.at is not None),
        default=None,
    )
    if first_start is None or last_finish is None:
        return 0.0
    return max(0.0, (last_finish - first_start).total_seconds())


def _drain(measured: list[Event], total_s: float) -> tuple[float, float]:
    finishes = sorted(
        e.at for e in measured if e.type == "enqueue_finished" and e.at is not None
    )
    terminals = sorted(
        e.at
        for e in measured
        if e.at is not None
        and (
            (e.type == "handler_finished" and e.result == "success")
            or e.type == "dead_lettered"
        )
    )
    if not finishes or not terminals:
        return 0.0, 0.0
    workload_end = finishes[-1]
    span = max(0.0, (terminals[-1] - workload_end).total_seconds())
    drained = sum(1 for at in terminals if at >= workload_end)
    return span, _rate(drained, span, total_s)


def _startup_ms(events: list[Event]) -> float:
    ready = sorted(
        e.at
        for e in events
        if e.type == "process_ready" and e.role == "worker" and e.at is not None
    )
    first_execution = sorted(
        e.at for e in events if e.type == "handler_started" and e.at is not None
    )
    if not ready or not first_execution:
        return 0.0
    return max(0.0, (first_execution[0] - ready[0]).total_seconds() * 1000)


def compute_retry_lags(events: list[Event], retry_delay_ms: float) -> list[float]:
    starts: dict[int, list[datetime.datetime]] = {}
    finishes: dict[int, list[datetime.datetime]] = {}
    for event in events:
        if event.workload_id is None or event.at is None:
            continue
        if event.type == "handler_started":
            starts.setdefault(event.workload_id, []).append(event.at)
        elif event.type == "handler_finished" and event.result != "success":
            finishes.setdefault(event.workload_id, []).append(event.at)

    lags: list[float] = []
    for workload_id, start_times in starts.items():
        if len(start_times) < 2:
            continue
        start_times = sorted(start_times)
        finish_times = sorted(finishes.get(workload_id, []))
        if not finish_times:
            continue
        # First retry gap beyond the configured delay approximates mover lag.
        lag_ms = (start_times[1] - finish_times[0]).total_seconds() * 1000 - retry_delay_ms
        lags.append(max(0.0, lag_ms))
    return lags

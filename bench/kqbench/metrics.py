"""Throughput and latency aggregation."""

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
    duration_seconds: float,
) -> dict[str, Any]:
    events: list[Event]
    if raw_events and isinstance(raw_events[0], dict):
        events = parse_events(raw_events)  # type: ignore[arg-type]
    else:
        events = list(raw_events)  # type: ignore[arg-type]

    requested = int(config["workload"]["arrival"]["target_rps"])
    retry_delay_ms = float(config["kq"].get("retry_delay_ms", 0))

    enqueue_ok = [e for e in events if e.type == "enqueue_finished" and e.result == "success"]
    enqueue_times = sorted(e.at for e in enqueue_ok if e.at is not None)
    if len(enqueue_times) >= 2:
        span = (enqueue_times[-1] - enqueue_times[0]).total_seconds()
        span = max(span, 1e-3)
        achieved = len(enqueue_times) / span
    elif enqueue_times:
        achieved = float(len(enqueue_times)) / max(duration_seconds, 1e-3)
    else:
        achieved = 0.0

    all_times = sorted(e.at for e in events if e.at is not None)
    if len(all_times) >= 2:
        total_span = max((all_times[-1] - all_times[0]).total_seconds(), 1e-3)
    else:
        total_span = max(duration_seconds, 1e-3)

    completed = correctness.completed
    dead_lettered = correctness.dead_lettered
    handler_errors = sum(
        1 for e in events if e.type == "handler_finished" and e.result != "success"
    )
    # Derived mover throughput: each retry schedules one error plus one move,
    # while each DLQ task contributes a final error with no move.
    retry_moves = max(0, handler_errors - dead_lettered)

    enqueue_latencies = [e.duration_ns / 1e6 for e in enqueue_ok if e.duration_ns > 0]

    enqueue_at: dict[int, datetime.datetime] = {}
    for event in enqueue_ok:
        if event.workload_id is not None and event.at is not None:
            enqueue_at.setdefault(event.workload_id, event.at)

    terminal_at: dict[int, datetime.datetime] = {}
    for event in events:
        if event.workload_id is None or event.at is None:
            continue
        if event.type == "handler_finished" and event.result == "success":
            terminal_at.setdefault(event.workload_id, event.at)
        elif event.type == "dead_lettered":
            terminal_at.setdefault(event.workload_id, event.at)

    deliveries: list[float] = []
    for workload_id, finished in terminal_at.items():
        started = enqueue_at.get(workload_id)
        if started is not None:
            deliveries.append(max(0.0, (finished - started).total_seconds() * 1000))

    executions = [
        e.duration_ns / 1e6
        for e in events
        if e.type == "handler_finished" and e.duration_ns > 0
    ]

    retry_lags = compute_retry_lags(events, retry_delay_ms)

    execution_avg = sum(executions) / len(executions) if executions else 0

    return {
        "duration_ms": int(duration_seconds * 1000),
        "throughput": {
            "requested_enqueue_per_second": requested,
            "achieved_enqueue_per_second": achieved,
            "completed_per_second": completed / total_span if total_span else 0,
            "retry_moves_per_second": retry_moves / total_span if total_span else 0,
            "dead_lettered_per_second": dead_lettered / total_span if total_span else 0,
        },
        "latency_ms": {
            "enqueue_p50": percentile(enqueue_latencies, 0.50),
            "enqueue_p95": percentile(enqueue_latencies, 0.95),
            "enqueue_p99": percentile(enqueue_latencies, 0.99),
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

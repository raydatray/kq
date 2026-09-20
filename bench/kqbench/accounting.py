from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Correctness:
    produced: int = 0
    completed: int = 0
    dead_lettered: int = 0
    outstanding: list[int] = field(default_factory=list)
    duplicates: int = 0
    missing: list[int] = field(default_factory=list)
    enqueue_errors: int = 0


def _type(event: Any) -> str:
    if isinstance(event, dict):
        return str(event.get("type", ""))
    return str(getattr(event, "type", ""))


def _workload_id(event: Any) -> int | None:
    if isinstance(event, dict):
        value = event.get("workload_id")
    else:
        value = getattr(event, "workload_id", None)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _result(event: Any) -> str:
    if isinstance(event, dict):
        return str(event.get("result", "") or "")
    return str(getattr(event, "result", "") or "")


def _error(event: Any) -> str:
    if isinstance(event, dict):
        return str(event.get("error", "") or "")
    return str(getattr(event, "error", "") or "")


def account(events: list[Any], exclude: set[int] | frozenset[int] | None = None) -> Correctness:
    excluded = exclude or frozenset()
    produced: set[int] = set()
    enqueue_errors = 0
    completed: set[int] = set()
    dead_lettered: set[int] = set()
    starts: Counter[int] = Counter()

    for event in events:
        event_type = _type(event)
        workload_id = _workload_id(event)
        if workload_id is not None and workload_id in excluded:
            continue
        if event_type == "enqueue_finished":
            if _result(event) == "success" and not _error(event):
                if workload_id is not None:
                    produced.add(workload_id)
            else:
                if workload_id is not None or _error(event):
                    enqueue_errors += 1
        elif event_type == "handler_started":
            if workload_id is not None:
                starts[workload_id] += 1
        elif event_type == "handler_finished":
            if _result(event) == "success" and workload_id is not None:
                completed.add(workload_id)
        elif event_type == "dead_lettered":
            if workload_id is not None:
                dead_lettered.add(workload_id)

    # Restrict terminal sets to produced IDs so stale DLQ observations from
    # a reused topic cannot mask missing work.
    completed &= produced
    dead_lettered &= produced
    duplicates = sum(max(0, count - 1) for count in starts.values())

    unresolved = produced - completed - dead_lettered
    started = set(starts.keys())
    outstanding = sorted(unresolved & started)
    missing = sorted(unresolved - started)

    return Correctness(
        produced=len(produced),
        completed=len(completed),
        dead_lettered=len(dead_lettered),
        outstanding=outstanding,
        duplicates=duplicates,
        missing=missing,
        enqueue_errors=enqueue_errors,
    )

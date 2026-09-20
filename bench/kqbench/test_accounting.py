from __future__ import annotations

import datetime
import unittest

from .accounting import account
from .metrics import percentile


def event(
    event_type: str,
    workload_id: int | None = None,
    result: str = "",
    error: str = "",
) -> dict:
    payload: dict = {"type": event_type}
    if workload_id is not None:
        payload["workload_id"] = workload_id
    if result:
        payload["result"] = result
    if error:
        payload["error"] = error
    payload["at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return payload


class AccountingTest(unittest.TestCase):
    def test_empty(self) -> None:
        correctness = account([])
        self.assertEqual(correctness.produced, 0)
        self.assertEqual(correctness.missing, [])
        self.assertEqual(correctness.outstanding, [])

    def test_success_path(self) -> None:
        events = [
            event("enqueue_finished", 1, result="success"),
            event("handler_started", 1),
            event("handler_finished", 1, result="success"),
        ]
        correctness = account(events)
        self.assertEqual(correctness.produced, 1)
        self.assertEqual(correctness.completed, 1)
        self.assertEqual(correctness.duplicates, 0)
        self.assertEqual(correctness.missing, [])
        self.assertEqual(correctness.outstanding, [])

    def test_duplicates_counted(self) -> None:
        events = [
            event("enqueue_finished", 1, result="success"),
            event("handler_started", 1),
            event("handler_started", 1),
            event("handler_finished", 1, result="success"),
        ]
        correctness = account(events)
        self.assertEqual(correctness.duplicates, 1)
        self.assertEqual(correctness.missing, [])

    def test_missing_and_outstanding_split(self) -> None:
        events = [
            event("enqueue_finished", 1, result="success"),
            event("enqueue_finished", 2, result="success"),
            event("handler_started", 1),
        ]
        correctness = account(events)
        self.assertEqual(correctness.outstanding, [1])
        self.assertEqual(correctness.missing, [2])

    def test_dead_lettered_terminal(self) -> None:
        events = [
            event("enqueue_finished", 1, result="success"),
            event("handler_started", 1),
            event("handler_finished", 1, result="error", error="boom"),
            event("dead_lettered", 1),
        ]
        correctness = account(events)
        self.assertEqual(correctness.dead_lettered, 1)
        self.assertEqual(correctness.missing, [])
        self.assertEqual(correctness.outstanding, [])

    def test_enqueue_errors_tracked(self) -> None:
        events = [event("enqueue_finished", 1, result="error", error="down")]
        correctness = account(events)
        self.assertEqual(correctness.produced, 0)
        self.assertEqual(correctness.enqueue_errors, 1)

    def test_stale_dlq_ignored(self) -> None:
        events = [event("dead_lettered", 99)]
        correctness = account(events)
        self.assertEqual(correctness.dead_lettered, 0)

    def test_excluded_ids_skipped(self) -> None:
        warmup = 1 << 32
        events = [
            event("enqueue_finished", 1, result="success"),
            event("handler_started", 1),
            event("handler_finished", 1, result="success"),
            event("enqueue_finished", warmup, result="success"),
            event("handler_started", warmup),
            event("handler_finished", warmup, result="success"),
        ]
        correctness = account(events, exclude=frozenset({warmup}))
        self.assertEqual(correctness.produced, 1)
        self.assertEqual(correctness.completed, 1)
        self.assertEqual(correctness.duplicates, 0)
        self.assertEqual(correctness.missing, [])


class PercentileTest(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(percentile([], 0.95), 0)

    def test_quantiles(self) -> None:
        values = [float(value) for value in range(1, 101)]
        self.assertEqual(percentile(values, 0.50), 50)
        self.assertEqual(percentile(values, 0.95), 95)
        self.assertEqual(percentile(values, 0.99), 99)


if __name__ == "__main__":
    unittest.main()

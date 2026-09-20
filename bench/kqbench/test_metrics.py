from __future__ import annotations

import datetime
import unittest

from .accounting import account
from .metrics import summarize

WARMUP_ID = 1 << 32
T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def timed(
    event_type: str,
    offset_seconds: float,
    workload_id: int | None = None,
    role: str = "",
    result: str = "",
    duration_ns: int = 0,
) -> dict:
    payload: dict = {
        "type": event_type,
        "role": role,
        "process": 0,
        "at": (T0 + datetime.timedelta(seconds=offset_seconds)).isoformat(),
    }
    if workload_id is not None:
        payload["workload_id"] = workload_id
    if result:
        payload["result"] = result
    if duration_ns:
        payload["duration_ns"] = duration_ns
    return payload


def test_config() -> dict:
    return {
        "workload": {"arrival": {"target_rps": 100}},
        "kq": {"retry_delay_ms": 1000},
    }


def scenario_events() -> list[dict]:
    return [
        timed("process_ready", 0.0, role="worker"),
        timed("enqueue_started", 1.0, WARMUP_ID, role="producer"),
        timed("enqueue_finished", 1.01, WARMUP_ID, role="producer", result="success", duration_ns=1_000_000),
        timed("handler_started", 5.0, WARMUP_ID, role="worker"),
        timed("handler_finished", 5.1, WARMUP_ID, role="worker", result="success", duration_ns=100_000_000),
        timed("enqueue_started", 6.0, 1, role="producer"),
        timed("enqueue_finished", 6.1, 1, role="producer", result="success", duration_ns=2_000_000),
        timed("handler_started", 7.0, 1, role="worker"),
        timed("handler_finished", 7.2, 1, role="worker", result="success", duration_ns=10_000_000),
        timed("enqueue_started", 6.0, 2, role="producer"),
        timed("enqueue_finished", 8.0, 2, role="producer", result="success", duration_ns=2_000_000),
        timed("handler_started", 8.1, 2, role="worker"),
        timed("handler_finished", 8.15, 2, role="worker", result="success", duration_ns=10_000_000),
    ]


class SummarizeTest(unittest.TestCase):
    def test_windows_exclude_warmup(self) -> None:
        events = scenario_events()
        correctness = account(events, exclude=frozenset({WARMUP_ID}))
        result = summarize(
            test_config(), events, correctness,
            total_duration_ms=20000, warmup_ids=frozenset({WARMUP_ID}),
        )
        windows = result["windows_ms"]
        self.assertAlmostEqual(windows["enqueue"], 2000.0)
        self.assertAlmostEqual(windows["active"], 1150.0)
        self.assertAlmostEqual(windows["drain"], 150.0)
        self.assertAlmostEqual(windows["startup"], 5000.0)
        self.assertEqual(result["duration_ms"], 20000)

    def test_windowed_throughput(self) -> None:
        events = scenario_events()
        correctness = account(events, exclude=frozenset({WARMUP_ID}))
        result = summarize(
            test_config(), events, correctness,
            total_duration_ms=20000, warmup_ids=frozenset({WARMUP_ID}),
        )
        throughput = result["throughput"]
        self.assertEqual(throughput["requested_enqueue_per_second"], 100)
        self.assertAlmostEqual(throughput["achieved_enqueue_per_second"], 1.0)
        self.assertAlmostEqual(throughput["active_worker_per_second"], 2 / 1.15)
        self.assertAlmostEqual(throughput["drain_per_second"], 1 / 0.15)
        self.assertEqual(throughput["retry_moves_per_second"], 0.0)
        self.assertEqual(throughput["dead_lettered_per_second"], 0.0)

    def test_queue_and_delivery_latency(self) -> None:
        events = scenario_events()
        correctness = account(events, exclude=frozenset({WARMUP_ID}))
        result = summarize(
            test_config(), events, correctness,
            total_duration_ms=20000, warmup_ids=frozenset({WARMUP_ID}),
        )
        latency = result["latency_ms"]
        self.assertAlmostEqual(latency["queue_p50"], 100.0)
        self.assertAlmostEqual(latency["queue_p95"], 900.0)
        self.assertAlmostEqual(latency["delivery_p50"], 150.0)
        self.assertAlmostEqual(latency["delivery_p95"], 1100.0)
        self.assertAlmostEqual(latency["execution_average"], 10.0)
        self.assertAlmostEqual(latency["execution_p95"], 10.0)

    def test_empty_events(self) -> None:
        correctness = account([])
        result = summarize(test_config(), [], correctness, total_duration_ms=5000)
        self.assertEqual(result["duration_ms"], 5000)
        self.assertEqual(result["windows_ms"], {"enqueue": 0.0, "active": 0.0, "drain": 0.0, "startup": 0.0})
        self.assertEqual(result["throughput"]["achieved_enqueue_per_second"], 0.0)
        self.assertEqual(result["latency_ms"]["queue_p95"], 0)

    def test_enqueue_window_without_started_events(self) -> None:
        events = [
            timed("enqueue_finished", 1.0, 1, role="producer", result="success", duration_ns=1_000_000),
            timed("enqueue_finished", 3.0, 2, role="producer", result="success", duration_ns=1_000_000),
        ]
        correctness = account(events)
        result = summarize(test_config(), events, correctness, total_duration_ms=10000)
        self.assertAlmostEqual(result["windows_ms"]["enqueue"], 2000.0)
        self.assertAlmostEqual(result["throughput"]["achieved_enqueue_per_second"], 1.0)


if __name__ == "__main__":
    unittest.main()

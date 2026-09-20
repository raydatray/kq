"""Harness run lifecycle: Kafka, processes, accounting, artifacts."""

from __future__ import annotations

import datetime
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .config import write_json
from .kafka import KafkaEnvironment
from .processes import (
    ManagedProcess,
    build_go_runner,
    collect_events,
    start_role,
    stop_processes,
    wait_for_processes,
    wait_until_ready,
)


def run_harness(config: dict[str, Any], output: Path) -> int:
    kafka = KafkaEnvironment(config)
    processes: list[ManagedProcess] = []
    run_dir = unique_run_dir(output, str(config["run_id"]))
    config["run_id"] = run_dir.name
    log_dir = run_dir / "logs"
    timeouts = config.get("timeouts", {})
    startup = float(timeouts.get("startup_seconds", 30))
    workload_timeout = float(timeouts.get("workload_seconds", 180))
    drain_timeout = float(timeouts.get("drain_seconds", 90))
    shutdown = float(timeouts.get("shutdown_seconds", 10))

    started = time.time()
    try:
        run_dir.mkdir(parents=True, exist_ok=False)
        log_dir.mkdir(parents=True, exist_ok=True)
        resolved_path = run_dir / "run.json"
        write_json(resolved_path, config)

        kafka.start()
        kafka.provision()

        binary = build_go_runner()
        processes += start_role(binary, "observer", config, resolved_path, log_dir)
        processes += start_role(binary, "worker", config, resolved_path, log_dir)
        processes += start_role(binary, "mover", config, resolved_path, log_dir)
        wait_until_ready(processes, timeout=startup)

        anchor_outage_window(config, resolved_path)

        producers = start_role(binary, "producer", config, resolved_path, log_dir)
        processes += producers
        wait_until_ready(producers, timeout=startup)
        wait_for_processes(producers, timeout=workload_timeout)
        wait_for_terminal_accounting(processes, config, timeout=drain_timeout)

        return write_results(config, processes, run_dir, started)
    except Exception as exc:  # noqa: BLE001 - harness must retain diagnostics
        try:
            write_failure(run_dir, config, processes, started, exc)
        except Exception:
            pass
        print(f"kq-bench: run failed: {exc}")
        return 1
    finally:
        stop_processes(processes, grace_seconds=shutdown)
        try:
            kafka.capture_diagnostics(log_dir)
        except Exception:
            pass
        kafka.stop()


def wait_for_terminal_accounting(
    processes: list[ManagedProcess],
    config: dict[str, Any],
    timeout: float,
) -> None:
    # Imported late so orchestration modules stay importable standalone.
    from .accounting import account

    total = int(config["workload"]["total_tasks"])
    deadline = time.time() + timeout
    last: Any = None
    while True:
        events = collect_events(processes)
        last = account(events)
        attempted = last.produced + last.enqueue_errors
        if attempted >= total and not last.missing and not last.outstanding:
            return
        if time.time() >= deadline:
            raise TimeoutError(
                f"drain timed out: produced={last.produced} completed={last.completed} "
                f"dead_lettered={last.dead_lettered} outstanding={len(last.outstanding)} "
                f"missing={len(last.missing)}"
            )
        time.sleep(1.0)


def write_results(
    config: dict[str, Any],
    processes: list[ManagedProcess],
    run_dir: Path,
    started: float,
) -> int:
    from .accounting import account
    from .artifacts import write_artifacts
    from .metrics import summarize

    events = collect_events(processes)
    correctness = account(events)
    result = summarize(config, events, correctness, time.time() - started)
    write_artifacts(run_dir, config, result)

    if correctness.missing or correctness.outstanding:
        print(
            "kq-bench: correctness failed: "
            f"missing={len(correctness.missing)} outstanding={len(correctness.outstanding)}"
        )
        return 1
    return 0


def write_failure(
    run_dir: Path,
    config: dict[str, Any],
    processes: list[ManagedProcess],
    started: float,
    exc: BaseException,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    if not (run_dir / "run.json").exists():
        write_json(run_dir / "run.json", config)
    try:
        from .accounting import account
        from .artifacts import write_artifacts
        from .metrics import summarize

        events = collect_events(processes)
        correctness = account(events)
        result = summarize(config, events, correctness, time.time() - started)
        result["error"] = str(exc)
        write_artifacts(run_dir, config, result)
    except Exception:
        (run_dir / "error.txt").write_text(str(exc), encoding="utf-8")


def anchor_outage_window(config: dict[str, Any], resolved_path: Path) -> None:
    """Anchor until_time outages to workload start.

    Workers and movers already run but only producers consume the outage
    timestamp (via generated job payloads), so rewriting the resolved file
    here is safe and keeps run.json accurate.
    """
    failures = config["workload"]["failures"]
    if failures.get("mode") != "until_time":
        return
    if int(failures.get("duration_ms", 0)) <= 0:
        return
    if failures.get("until"):
        return
    until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        milliseconds=int(failures["duration_ms"])
    )
    failures["until"] = until.isoformat().replace("+00:00", "Z")
    write_json(resolved_path, config)


def unique_run_dir(output: Path, run_id: str) -> Path:
    candidate = output / run_id
    if not candidate.exists():
        return candidate
    suffix = 2
    while (output / f"{run_id}-{suffix}").exists():
        suffix += 1
    return output / f"{run_id}-{suffix}"

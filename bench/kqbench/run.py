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
    start_warmup_producer,
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
    warmup_timeout = float(timeouts.get("warmup_seconds", 60))
    workload_timeout = float(timeouts.get("workload_seconds", 180))
    drain_timeout = float(timeouts.get("drain_seconds", 90))
    shutdown = float(timeouts.get("shutdown_seconds", 10))
    warmup_ids = warmup_id_set(config)

    entry = time.time()
    kafka_start: float | None = None
    error: Exception | None = None
    try:
        run_dir.mkdir(parents=True, exist_ok=False)
        log_dir.mkdir(parents=True, exist_ok=True)
        resolved_path = run_dir / "run.json"
        write_json(resolved_path, config)

        kafka_start = time.time()
        kafka.start()
        kafka.provision()

        binary = build_go_runner()
        processes += start_role(binary, "observer", config, resolved_path, log_dir)
        processes += start_role(binary, "worker", config, resolved_path, log_dir)
        processes += start_role(binary, "mover", config, resolved_path, log_dir)
        wait_until_ready(processes, timeout=startup)

        run_warmup_phase(
            binary, processes, warmup_ids, resolved_path, log_dir, timeout=warmup_timeout
        )

        anchor_outage_window(config, resolved_path)

        producers = start_role(binary, "producer", config, resolved_path, log_dir)
        processes += producers
        wait_until_ready(producers, timeout=startup)
        wait_for_processes(producers, timeout=workload_timeout)
        wait_for_terminal_accounting(processes, config, warmup_ids, timeout=drain_timeout)
    except Exception as exc:  # noqa: BLE001 - harness must retain diagnostics
        error = exc
    finally:
        stop_processes(processes, grace_seconds=shutdown)
        try:
            kafka.capture_diagnostics(log_dir)
        except Exception:
            pass
        kafka.stop()
        ended = time.time()

    t0 = kafka_start if kafka_start is not None else entry
    total_ms = int((ended - t0) * 1000)
    return write_results(config, processes, run_dir, warmup_ids, total_ms, error)


def run_sweep(
    definition: dict[str, Any],
    runs: list[dict[str, Any]],
    output: Path,
    *,
    run_one: Callable[[dict[str, Any], Path], int] | None = None,
) -> int:
    from .artifacts import write_sweep_manifest

    if not runs:
        raise ValueError("sweep has no resolved runs")
    if run_one is None:
        run_one = run_harness

    first = runs[0]
    date = str(first["created_at"])[:10]
    revision = str(first["revision"])
    sweep_dir = unique_run_dir(output, f"{date}-{definition['id']}-{revision[:7]}")
    manifest: dict[str, Any] = {
        "id": definition["id"],
        "description": definition["description"],
        "scenario": definition["scenario"],
        "profile": first["profile"],
        "created_at": first["created_at"],
        "revision": revision,
        "target_rps": list(definition["target_rps"]),
        "repetitions": int(definition["repetitions"]),
        "runs": [],
    }
    write_sweep_manifest(sweep_dir, manifest)

    status = 0
    for config in runs:
        run_status = int(run_one(config, output))
        status |= run_status
        point = config["sweep"]
        manifest["runs"].append({
            "target_rps": int(point["target_rps"]),
            "repetition": int(point["repetition"]),
            "run_id": config["run_id"],
            "status": run_status,
        })
        write_sweep_manifest(sweep_dir, manifest)

    return status


def run_warmup_phase(
    binary: Path,
    processes: list[ManagedProcess],
    warmup_ids: frozenset[int],
    resolved_path: Path,
    log_dir: Path,
    timeout: float,
) -> None:
    if not warmup_ids:
        return

    warmup = start_warmup_producer(binary, resolved_path, log_dir)
    processes.append(warmup)
    wait_until_ready([warmup], timeout=timeout)
    wait_for_processes([warmup], timeout=timeout)

    deadline = time.time() + timeout
    while True:
        finished = {
            int(event["workload_id"])
            for event in collect_events(processes)
            if event.get("type") == "handler_finished"
            and event.get("result") == "success"
            and event.get("workload_id") in warmup_ids
        }
        if warmup_ids.issubset(finished):
            return
        if time.time() >= deadline:
            pending = sorted(warmup_ids - finished)
            raise TimeoutError(
                f"warm-up timed out with {len(pending)} tasks unexecuted "
                f"(share group never became usable); pending={pending[:10]}"
            )
        time.sleep(0.5)


def wait_for_terminal_accounting(
    processes: list[ManagedProcess],
    config: dict[str, Any],
    warmup_ids: frozenset[int],
    timeout: float,
) -> None:
    from .accounting import account

    total = int(config["workload"]["total_tasks"])
    deadline = time.time() + timeout
    last: Any = None
    while True:
        events = collect_events(processes)
        last = account(events, exclude=warmup_ids)
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
    warmup_ids: frozenset[int],
    total_ms: int,
    error: Exception | None,
) -> int:
    from .accounting import account
    from .artifacts import write_artifacts
    from .metrics import summarize

    run_dir.mkdir(parents=True, exist_ok=True)
    if not (run_dir / "run.json").exists():
        write_json(run_dir / "run.json", config)
    try:
        events = collect_events(processes)
        correctness = account(events, exclude=warmup_ids)
        result = summarize(
            config, events, correctness, total_duration_ms=total_ms, warmup_ids=warmup_ids
        )
        if error is not None:
            result["error"] = str(error)
        write_artifacts(run_dir, config, result)
    except Exception as write_error:
        (run_dir / "error.txt").write_text(
            f"{error}\nartifact write failed: {write_error}", encoding="utf-8"
        )
        print(f"kq-bench: run failed: {error}")
        return 1

    if error is not None:
        print(f"kq-bench: run failed: {error}")
        return 1
    if correctness.missing or correctness.outstanding:
        print(
            "kq-bench: correctness failed: "
            f"missing={len(correctness.missing)} outstanding={len(correctness.outstanding)}"
        )
        return 1
    return 0


def warmup_id_set(config: dict[str, Any]) -> frozenset[int]:
    warmup = config.get("workload", {}).get("warmup", {})
    tasks = int(warmup.get("tasks", 0) or 0)
    base = int(warmup.get("id_base", 0) or 0)
    return frozenset(base + index for index in range(max(tasks, 0)))


def anchor_outage_window(config: dict[str, Any], resolved_path: Path) -> None:
    """Anchor until_time failures after workers and movers have started."""
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

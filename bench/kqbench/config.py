"""Resolved run configuration for catalogue and ad hoc runs."""

from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

from .scenarios import PROFILES, SCENARIOS, deep_merge, load_json, parse_overrides

DEFAULT_KQ = {
    "brokers": ["localhost:19092"],
    "queue": "bench",
    "max_retries": 3,
    "retry_delay_ms": 1000,
    "retry_grid_boundaries_ms": [0, 120000],
    "retry_grid_partitions": 4,
}

DEFAULT_TIMEOUTS = {
    "startup_seconds": 30,
    "warmup_seconds": 60,
    "workload_seconds": 120,
    "drain_seconds": 90,
    "shutdown_seconds": 10,
}

# Warm-up workload IDs live in a disjoint range so measured accounting can
# exclude them by ID without timestamp cutoffs.
WARMUP_ID_BASE = 1 << 32


def resolve_run(args: argparse.Namespace) -> dict[str, Any]:
    profile = load_json(PROFILES / f"{args.profile}.json")

    if getattr(args, "scenario", None):
        scenario = load_json(SCENARIOS / f"{args.scenario}.json")
        scenario_info: dict[str, Any] | None = {
            "id": scenario["id"],
            "version": scenario.get("version", 1),
        }
        purpose = scenario.get("description", "")
        behavior: dict[str, Any] = dict(scenario.get("behavior", {}))
        adhoc: dict[str, Any] = {}
    else:
        if not getattr(args, "config", None):
            raise ValueError("either --scenario or --config is required")
        adhoc = load_json(Path(args.config))
        scenario_info = None
        purpose = adhoc.get("purpose", "")
        if not purpose:
            raise ValueError("ad hoc config requires a purpose")
        behavior = dict(adhoc.get("behavior", {}))

    # Profiles define scale; scenarios and ad hoc configs define behavior.
    arrival = dict(profile.get("arrival", {}))
    if "arrival" in adhoc:
        arrival = deep_merge(arrival, dict(adhoc["arrival"]))

    execution_time = dict(behavior.get("execution_time_ms", {}))
    if "execution_time_ms" in adhoc:
        execution_time = deep_merge(execution_time, dict(adhoc["execution_time_ms"]))
    if not execution_time:
        raise ValueError("workload behavior requires execution_time_ms")

    failures: dict[str, Any] = {
        "rate": behavior.get("failure_rate", 0),
        "mode": behavior.get("failure_mode", "none"),
        "duration_ms": behavior.get("outage_duration_ms", 0),
        "attempts": behavior.get("fail_attempts", 0),
    }
    if "failures" in adhoc:
        failures = deep_merge(failures, dict(adhoc["failures"]))

    topology_profile = dict(profile.get("topology", {}))
    topology: dict[str, Any] = {
        "ready_partitions": topology_profile.get("ready_partitions", 1),
        "retry_partitions": topology_profile.get("retry_partitions", 4),
        "producers": topology_profile.get("producers", 1),
        "workers": topology_profile.get("workers", 1),
        "movers": topology_profile.get("movers", 1),
    }
    if "topology" in adhoc:
        topology = deep_merge(topology, dict(adhoc["topology"]))

    random_seed = adhoc.get("random_seed", profile.get("random_seed", 1))
    warmup_tasks = adhoc.get("warmup_tasks", profile.get("warmup_tasks", 10))
    warmup = {"tasks": warmup_tasks, "id_base": WARMUP_ID_BASE}

    kq = dict(DEFAULT_KQ)
    if "kq" in adhoc:
        kq = deep_merge(kq, dict(adhoc["kq"]))

    timeouts = dict(DEFAULT_TIMEOUTS)
    # Scale workload timeout with arrival duration by default.
    try:
        duration_seconds = int(arrival.get("duration_seconds", 0))
    except (TypeError, ValueError):
        duration_seconds = 0
    timeouts["workload_seconds"] = duration_seconds + 120
    if "timeouts" in adhoc:
        timeouts = deep_merge(timeouts, dict(adhoc["timeouts"]))

    created_at = datetime.datetime.now(datetime.timezone.utc)
    revision = get_revision()

    scenario_label = scenario_info["id"] if scenario_info else "adhoc"
    run_id = build_run_id(created_at, str(scenario_label), revision)

    resolved: dict[str, Any] = {
        "run_id": run_id,
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "revision": revision,
        "purpose": purpose,
        "scenario": scenario_info,
        "profile": args.profile,
        "command": build_command(args),
        "environment": get_environment(),
        "topology": topology,
        "workload": {
            "arrival": arrival,
            "execution_time_ms": execution_time,
            "random_seed": random_seed,
            "failures": failures,
            "warmup": warmup,
        },
        "kq": kq,
        "timeouts": timeouts,
    }

    # Command-line overrides apply to the fully resolved structure so
    # topology, workload, kq, and timeouts can all be tuned per run.
    overrides = parse_overrides(getattr(args, "set", None))
    if overrides:
        resolved = deep_merge(resolved, overrides)

    # Recompute derived fields after overrides.
    workload = resolved["workload"]
    arrival = workload["arrival"]
    total_tasks = int(arrival["target_rps"]) * int(arrival["duration_seconds"])
    workload["total_tasks"] = total_tasks

    failures = workload["failures"]
    # The shared outage timestamp is anchored at workload start by run_harness
    # (not here) so temporary-outage runs fail during execution rather than
    # expiring during Kafka provisioning. Manual kq-load runs fall back to
    # producer-start + duration_ms when until is absent.
    failures.setdefault("duration_ms", 0)
    failures.setdefault("attempts", 0)
    failures.setdefault("until", None)

    warmup_section = workload.get("warmup", {})
    warmup_section.setdefault("tasks", 10)
    warmup_section.setdefault("id_base", WARMUP_ID_BASE)
    workload["warmup"] = warmup_section

    validate_resolved_config(resolved)
    return resolved


def resolve_sweep_runs(sweep: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for target_rps in sweep["target_rps"]:
        for repetition in range(1, int(sweep["repetitions"]) + 1):
            run_args = argparse.Namespace(
                scenario=sweep["scenario"],
                config=None,
                profile=args.profile,
                set=[*getattr(args, "set", []), f"workload.arrival.target_rps={target_rps}"],
                argv=getattr(args, "argv", None),
            )
            resolved = resolve_run(run_args)
            resolved["run_id"] += f"-{target_rps}rps-r{repetition}"
            resolved["sweep"] = {
                "id": sweep["id"],
                "target_rps": target_rps,
                "repetition": repetition,
            }
            runs.append(resolved)
    return runs


def validate_resolved_config(resolved: dict[str, Any]) -> None:
    try:
        workload = resolved["workload"]
        arrival = workload["arrival"]
        execution = workload["execution_time_ms"]
        failures = workload["failures"]
        topology = resolved["topology"]
        kq = resolved["kq"]
    except KeyError as exc:
        raise ValueError(f"resolved config missing {exc}") from exc

    if int(arrival["target_rps"]) <= 0:
        raise ValueError("arrival.target_rps must be positive")
    if int(arrival["duration_seconds"]) <= 0:
        raise ValueError("arrival.duration_seconds must be positive")
    for key in ("average", "p95", "p99"):
        if float(execution[key]) < 0:
            raise ValueError(f"execution_time_ms.{key} cannot be negative")
    if float(execution["p99"]) < float(execution["p95"]):
        raise ValueError("execution_time_ms.p99 cannot be below p95")
    if failures["mode"] not in ("none", "permanent", "until_time", "attempts"):
        raise ValueError(f"unknown failure mode {failures['mode']!r}")
    if not 0 <= float(failures["rate"]) <= 1:
        raise ValueError("failures.rate must be between 0 and 1")
    for key in ("producers", "workers", "movers"):
        if int(topology[key]) <= 0:
            raise ValueError(f"topology.{key} must be positive")
    if not kq.get("brokers"):
        raise ValueError("kq.brokers is required")
    if not kq.get("queue"):
        raise ValueError("kq.queue is required")
    if int(kq.get("max_retries", 0)) < 0:
        raise ValueError("kq.max_retries cannot be negative")
    warmup = workload.get("warmup", {})
    if int(warmup.get("tasks", 0)) < 0:
        raise ValueError("warmup.tasks cannot be negative")
    if int(warmup.get("id_base", 0)) <= 0:
        raise ValueError("warmup.id_base must be positive")


def build_run_id(created_at: datetime.datetime, scenario_label: str, revision: str) -> str:
    date = created_at.strftime("%Y-%m-%d")
    short = (revision or "unknown")[:7]
    safe_label = "".join(c if c.isalnum() or c in ("-", "_") else "-" for c in scenario_label)
    return f"{date}-{safe_label}-{short}"


def build_command(args: argparse.Namespace) -> str:
    argv = getattr(args, "argv", None)
    if argv is None:
        argv = sys.argv
    return " ".join(argv)


def get_revision() -> str:
    for command in (["sl", "id", "-i"], ["git", "rev-parse", "HEAD"]):
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    return "unknown"


def get_environment() -> dict[str, Any]:
    try:
        import multiprocessing

        cpu_count = multiprocessing.cpu_count()
    except Exception:
        cpu_count = 0
    memory_bytes = 0
    try:
        import resource

        # ru_maxrss is kilobytes on Linux, bytes on macOS; record raw value
        # with best-effort normalization documented in analysis.
        memory_bytes = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        memory_bytes = 0
    go_version = ""
    try:
        result = subprocess.run(
            ["go", "version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            go_version = result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return {
        "go_version": go_version,
        "kafka_version": "apache/kafka:4.3.1",
        "operating_system": platform.system(),
        "architecture": platform.machine(),
        "ci_provider": os.environ.get("CI_PROVIDER", os.environ.get("GITHUB_ACTIONS", "")),
        "runner": os.environ.get("RUNNER_NAME", platform.node()),
        "cpu_count": cpu_count,
        "memory_bytes": memory_bytes,
        "python_version": platform.python_version(),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

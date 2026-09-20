from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

BENCH_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = BENCH_ROOT / "scenarios"
PROFILES = BENCH_ROOT / "profiles"
SWEEPS = BENCH_ROOT / "sweeps"


def load_json(path: Path | str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top-level JSON must be an object")
    return data


def load_sweep(name: str) -> dict[str, Any]:
    sweep = load_json(SWEEPS / f"{name}.json")
    if sweep.get("id") != name:
        raise ValueError(f"sweep ID {sweep.get('id')!r} does not match {name!r}")
    validate_sweep(sweep)
    return sweep


def validate_sweep(sweep: dict[str, Any]) -> None:
    if not str(sweep.get("description", "")).strip():
        raise ValueError("sweep description is required")

    scenario = str(sweep.get("scenario", ""))
    if not scenario:
        raise ValueError("sweep scenario is required")
    if not (SCENARIOS / f"{scenario}.json").is_file():
        raise ValueError(f"sweep scenario {scenario!r} does not exist")

    points = sweep.get("target_rps")
    if not isinstance(points, list) or not points:
        raise ValueError("sweep target_rps must be a non-empty list")
    if any(not isinstance(point, int) or isinstance(point, bool) or point <= 0 for point in points):
        raise ValueError("sweep target_rps values must be positive integers")
    if any(current <= previous for previous, current in zip(points, points[1:])):
        raise ValueError("sweep target_rps values must be strictly increasing")

    repetitions = sweep.get("repetitions")
    if not isinstance(repetitions, int) or isinstance(repetitions, bool) or repetitions <= 0:
        raise ValueError("sweep repetitions must be a positive integer")


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def parse_overrides(entries: list[str] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for entry in entries or []:
        if "=" not in entry:
            raise ValueError(f"invalid --set {entry!r}: expected key=value")
        key, raw = entry.split("=", 1)
        keys = [part.strip() for part in key.split(".") if part.strip()]
        if not keys:
            raise ValueError(f"invalid --set {entry!r}: empty key")
        value = parse_value(raw)
        target = result
        for part in keys[:-1]:
            child = target.get(part)
            if not isinstance(child, dict):
                child = {}
                target[part] = child
            target = child
        target[keys[-1]] = value
    return result


def parse_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw

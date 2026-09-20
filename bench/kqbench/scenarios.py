"""Scenario and profile loading helpers."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

BENCH_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = BENCH_ROOT / "scenarios"
PROFILES = BENCH_ROOT / "profiles"


def load_json(path: Path | str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top-level JSON must be an object")
    return data


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override into base; override wins on conflict."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def parse_overrides(entries: list[str] | None) -> dict[str, Any]:
    """Parse --set dotted-key=value overrides into a nested dict."""
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

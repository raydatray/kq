from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Event:
    type: str
    role: str
    process: int
    workload_id: int | None
    at: datetime.datetime | None
    duration_ns: int
    result: str
    error: str


def parse_event(raw: dict[str, Any]) -> Event:
    at = raw.get("at")
    parsed_at: datetime.datetime | None = None
    if isinstance(at, str) and at:
        text = at.replace("Z", "+00:00") if at.endswith("Z") else at
        try:
            parsed_at = datetime.datetime.fromisoformat(text)
        except ValueError:
            parsed_at = None
    workload_id = raw.get("workload_id")
    return Event(
        type=str(raw.get("type", "")),
        role=str(raw.get("role", "")),
        process=int(raw.get("process", 0) or 0),
        workload_id=int(workload_id) if workload_id is not None else None,
        at=parsed_at,
        duration_ns=int(raw.get("duration_ns", 0) or 0),
        result=str(raw.get("result", "") or ""),
        error=str(raw.get("error", "") or ""),
    )


def parse_events(raws: list[dict[str, Any]]) -> list[Event]:
    events: list[Event] = []
    for raw in raws:
        if not isinstance(raw, dict):
            continue
        try:
            events.append(parse_event(raw))
        except (ValueError, TypeError):
            continue
    return events


def load_events(log_dir: Path) -> list[Event]:
    raws: list[dict[str, Any]] = []
    for path in sorted(log_dir.glob("*.log")):
        if path.name.endswith(".stderr"):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                raws.append(payload)
    return parse_events(raws)

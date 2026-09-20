"""Run artifact writing.

The runner writes machine artifacts only (`run.json`, `result.json`).
`analysis.md` is written manually by pointing an agent at the run
directory and `docs/performance/templates/analysis.md`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import write_json


def write_artifacts(directory: Path, run: dict[str, Any], result: dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    write_json(directory / "run.json", run)
    write_json(directory / "result.json", result)

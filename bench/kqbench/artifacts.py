from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import write_json


def write_artifacts(directory: Path, run: dict[str, Any], result: dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    write_json(directory / "run.json", run)
    write_json(directory / "result.json", result)


def write_sweep_manifest(directory: Path, manifest: dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    write_json(directory / "sweep.json", manifest)

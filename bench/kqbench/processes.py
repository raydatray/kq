from __future__ import annotations

import json
import os
import signal
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, TextIO

from .scenarios import BENCH_ROOT


class ManagedProcess:
    def __init__(
        self,
        role: str,
        index: int,
        proc: subprocess.Popen[str],
        event_log: Path,
        stderr_file: TextIO,
    ) -> None:
        self.role = role
        self.index = index
        self.proc = proc
        self.event_log = event_log
        self.stderr_file = stderr_file
        self._events: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._drain_stdout, daemon=True)
        self._reader.start()

    def _drain_stdout(self) -> None:
        try:
            with self.event_log.open("a", encoding="utf-8") as handle:
                assert self.proc.stdout is not None
                for line in self.proc.stdout:
                    handle.write(line)
                    handle.flush()
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(event, dict):
                        with self._lock:
                            self._events.append(event)
        except Exception:
            pass

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._events)

    def poll(self) -> int | None:
        return self.proc.poll()

    def wait(self, timeout: float | None = None) -> int:
        return self.proc.wait(timeout=timeout)

    def terminate(self) -> None:
        try:
            if self.proc.poll() is None:
                os.killpg(self.proc.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass

    def kill(self) -> None:
        try:
            if self.proc.poll() is None:
                os.killpg(self.proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass

    def close(self) -> None:
        try:
            if self.proc.stdout:
                self.proc.stdout.close()
        except Exception:
            pass
        try:
            self.stderr_file.close()
        except Exception:
            pass


def start_process(command: list[str], log: TextIO) -> subprocess.Popen[str]:
    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=log,
        text=True,
        start_new_session=True,
    )


def build_go_runner() -> Path:
    repo_root = BENCH_ROOT.parent
    output = Path(tempfile.gettempdir()) / f"kq-load-{os.getpid()}"
    result = subprocess.run(
        ["go", "build", "-o", str(output), "./bench/cmd/kq-load"],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode != 0:
        raise RuntimeError(f"go build kq-load failed: {result.stderr}")
    return output


def log_name(role: str, index: int) -> str:
    if role == "observer":
        return "observer.log"
    return f"{role}-{index}.log"


def start_role(
    binary: Path,
    role: str,
    config: dict[str, Any],
    resolved_path: Path,
    log_dir: Path,
) -> list[ManagedProcess]:
    log_dir.mkdir(parents=True, exist_ok=True)
    if role == "observer":
        count = 1
    else:
        count = int(config["topology"][f"{role}s"])
    processes: list[ManagedProcess] = []
    for index in range(count):
        event_log = log_dir / log_name(role, index)
        stderr_path = log_dir / (log_name(role, index) + ".stderr")
        stderr_file = stderr_path.open("w", encoding="utf-8")
        command = [str(binary), role, "--config", str(resolved_path), "--index", str(index)]
        proc = start_process(command, stderr_file)
        processes.append(ManagedProcess(role, index, proc, event_log, stderr_file))
    return processes


def collect_events(processes: list[ManagedProcess]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for process in processes:
        events.extend(process.events())
    return events


def wait_until_ready(processes: list[ManagedProcess], timeout: float) -> None:
    deadline = time.time() + timeout
    expected = {(process.role, process.index) for process in processes}
    while time.time() < deadline:
        ready = set()
        for process in processes:
            for event in process.events():
                if event.get("type") == "process_ready":
                    ready.add((event.get("role"), int(event.get("process", 0))))
            if process.poll() is not None and (process.role, process.index) not in ready:
                raise RuntimeError(
                    f"{process.role}-{process.index} exited before ready "
                    f"(code {process.poll()}); see {process.event_log}"
                )
        if expected.issubset(ready):
            return
        time.sleep(0.1)
    missing = sorted(expected - ready)
    raise TimeoutError(f"processes not ready within {timeout}s: {missing}")


def wait_for_processes(processes: list[ManagedProcess], timeout: float) -> None:
    deadline = time.time() + timeout
    for process in processes:
        remaining = max(0.0, deadline - time.time())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(
                f"{process.role}-{process.index} did not exit within {timeout}s"
            ) from exc


def stop_processes(processes: list[ManagedProcess], grace_seconds: float = 10) -> None:
    for process in processes:
        process.terminate()
    deadline = time.time() + grace_seconds
    for process in processes:
        remaining = max(0.0, deadline - time.time())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        process.close()

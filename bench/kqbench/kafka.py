"""Kafka environment lifecycle for harness runs."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from .scenarios import BENCH_ROOT

COMPOSE_FILE = BENCH_ROOT / "compose.yaml"
PROJECT_NAME = "kqbench"
KAFKA_IMAGE = "apache/kafka:4.3.1"


class KafkaEnvironment:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.queue: str = config["kq"]["queue"]
        self.ready_partitions: int = int(config["topology"].get("ready_partitions", 1))
        self.retry_partitions: int = int(config["topology"].get("retry_partitions", 4))

    def compose(self, *args: str) -> list[str]:
        return [
            "docker",
            "compose",
            "--file",
            str(COMPOSE_FILE),
            "--project-name",
            PROJECT_NAME,
            *args,
        ]

    def start(self) -> None:
        result = subprocess.run(
            self.compose("up", "--detach", "--wait"),
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode != 0:
            raise RuntimeError(f"kafka start failed: {result.stdout}\n{result.stderr}")

    def provision(self) -> None:
        boundaries = list(self.config["kq"].get("retry_grid_boundaries_ms", [0, 120000]))
        retry_topics = []
        for lower_ms in boundaries[:-1]:
            retry_topics.append(f"{self.queue}-retry-{format_delay_ms(int(lower_ms))}")

        self._create_topic(f"{self.queue}-ready", self.ready_partitions, {})
        for topic in retry_topics:
            self._create_topic(
                topic,
                self.retry_partitions,
                {"message.timestamp.type": "LogAppendTime"},
            )
        self._create_topic(f"{self.queue}-dlq", 1, {})

        # Share groups default to latest; harness replays from earliest.
        result = subprocess.run(
            self.compose(
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server",
                "localhost:19092",
                "--alter",
                "--entity-type",
                "groups",
                "--entity-name",
                f"kq.{self.queue}.workers",
                "--add-config",
                "share.auto.offset.reset=earliest",
            ),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise RuntimeError(f"share group config failed: {result.stderr}")

    def _create_topic(self, topic: str, partitions: int, configs: dict[str, str]) -> None:
        command = self.compose(
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        )
        for key, value in configs.items():
            command += ["--config", f"{key}={value}"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"create topic {topic} failed: {result.stderr}")

    def capture_diagnostics(self, output: Path) -> None:
        try:
            output.mkdir(parents=True, exist_ok=True)
            logs = subprocess.run(
                self.compose("logs", "--no-color"),
                capture_output=True,
                text=True,
                timeout=30,
            )
            (output / "kafka.log").write_text(
                (logs.stdout or "") + (logs.stderr or ""), encoding="utf-8"
            )
            describe = subprocess.run(
                self.compose(
                    "exec",
                    "-T",
                    "kafka",
                    "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server",
                    "localhost:19092",
                    "--describe",
                ),
                capture_output=True,
                text=True,
                timeout=30,
            )
            (output / "kafka-topics.txt").write_text(
                (describe.stdout or "") + (describe.stderr or ""), encoding="utf-8"
            )
        except Exception as exc:  # noqa: BLE001 - diagnostics must not fail runs
            try:
                (output / "kafka-diagnostics-error.txt").write_text(
                    str(exc), encoding="utf-8"
                )
            except Exception:
                pass

    def stop(self) -> None:
        try:
            subprocess.run(
                self.compose("down", "--volumes"),
                capture_output=True,
                text=True,
                timeout=120,
            )
        except Exception:
            pass


def format_delay_ms(delay_ms: int) -> str:
    if delay_ms == 0:
        return "0s"
    seconds = delay_ms // 1000
    day = 24 * 3600
    if seconds % day == 0:
        return f"{seconds // day}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"

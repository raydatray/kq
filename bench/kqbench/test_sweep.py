from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .cli import parse_args
from .config import resolve_run, resolve_sweep_runs
from .run import run_sweep
from .scenarios import SCENARIOS, load_json, load_sweep, validate_sweep


class SweepCatalogueTest(unittest.TestCase):
    def test_loads_north_star_sweep(self) -> None:
        sweep = load_sweep("north-star-rps-v1")

        self.assertEqual(sweep["scenario"], "north-star-steady-v1")
        self.assertIn(20000, sweep["target_rps"])
        self.assertEqual(sweep["repetitions"], 3)

    def test_rejects_invalid_sweep(self) -> None:
        base = {
            "id": "test",
            "description": "test sweep",
            "scenario": "ready-success-v1",
            "target_rps": [100, 200],
            "repetitions": 1,
        }
        invalid = [
            {**base, "description": ""},
            {**base, "scenario": "missing"},
            {**base, "target_rps": []},
            {**base, "target_rps": [100, 100]},
            {**base, "target_rps": [0]},
            {**base, "repetitions": 0},
        ]

        for sweep in invalid:
            with self.subTest(sweep=sweep):
                with self.assertRaises(ValueError):
                    validate_sweep(sweep)


class SweepResolutionTest(unittest.TestCase):
    @patch("kqbench.config.get_environment", return_value={})
    @patch("kqbench.config.get_revision", return_value="abcdef123456")
    def test_catalogue_scenarios_resolve(self, _revision: object, _environment: object) -> None:
        for path in sorted(SCENARIOS.glob("*.json")):
            with self.subTest(scenario=path.stem):
                args = parse_args([
                    "run",
                    "--scenario", path.stem,
                    "--profile", "ci-smoke",
                ])
                resolved = resolve_run(args)
                scenario = load_json(path)
                self.assertEqual(resolved["purpose"], scenario["description"])
                self.assertEqual(
                    resolved["workload"]["execution_time_ms"],
                    scenario["behavior"]["execution_time_ms"],
                )

    def test_cli_parses_sweep(self) -> None:
        args = parse_args([
            "sweep",
            "--name", "north-star-rps-v1",
            "--profile", "local-baseline",
        ])

        self.assertEqual(args.command, "sweep")
        self.assertEqual(args.name, "north-star-rps-v1")
        self.assertEqual(args.profile, "local-baseline")

    @patch("kqbench.config.get_environment", return_value={})
    @patch("kqbench.config.get_revision", return_value="abcdef123456")
    def test_resolves_independent_runs(self, _revision: object, _environment: object) -> None:
        args = parse_args([
            "sweep",
            "--name", "north-star-rps-v1",
            "--profile", "local-baseline",
        ])
        sweep = load_sweep(args.name)
        runs = resolve_sweep_runs(sweep, args)

        self.assertEqual(len(runs), 21)
        self.assertEqual(len({run["run_id"] for run in runs}), 21)
        self.assertEqual(runs[0]["workload"]["arrival"]["target_rps"], 5000)
        self.assertEqual(runs[-1]["workload"]["arrival"]["target_rps"], 25000)
        self.assertEqual(runs[0]["profile"], "local-baseline")
        self.assertEqual(runs[-1]["sweep"]["repetition"], 3)


class SweepRunTest(unittest.TestCase):
    def test_runs_every_point_and_writes_manifest(self) -> None:
        definition = {
            "id": "test-sweep",
            "description": "test sweep",
            "scenario": "ready-success-v1",
            "target_rps": [100, 200],
            "repetitions": 1,
        }
        runs = [
            {
                "run_id": "run-100",
                "created_at": "2026-09-20T00:00:00Z",
                "revision": "abcdef123456",
                "profile": "ci-smoke",
                "sweep": {"id": "test-sweep", "target_rps": 100, "repetition": 1},
            },
            {
                "run_id": "run-200",
                "created_at": "2026-09-20T00:00:00Z",
                "revision": "abcdef123456",
                "profile": "ci-smoke",
                "sweep": {"id": "test-sweep", "target_rps": 200, "repetition": 1},
            },
        ]
        called: list[str] = []

        def run_one(config: dict, _output: Path) -> int:
            called.append(str(config["run_id"]))
            return int(config["run_id"] == "run-200")

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            status = run_sweep(definition, runs, output, run_one=run_one)
            manifests = list(output.glob("*/sweep.json"))

            self.assertEqual(status, 1)
            self.assertEqual(called, ["run-100", "run-200"])
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            self.assertEqual([run["status"] for run in manifest["runs"]], [0, 1])
            self.assertEqual([run["run_id"] for run in manifest["runs"]], called)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

from .cli import parse_args
from .config import resolve_run


class ResolveRunTest(unittest.TestCase):
    def test_profiles_resolve_worker_concurrency(self) -> None:
        baseline = resolve_run(parse_args([
            "run", "--scenario", "ready-success-v1", "--profile", "local-baseline",
        ]))
        smoke = resolve_run(parse_args([
            "run", "--scenario", "ready-success-v1", "--profile", "ci-smoke",
        ]))

        self.assertEqual(baseline["topology"]["worker_concurrency"], 1)
        self.assertEqual(smoke["topology"]["worker_concurrency"], 2)

    def test_override_worker_concurrency(self) -> None:
        resolved = resolve_run(parse_args([
            "run", "--scenario", "ready-success-v1", "--profile", "local-baseline",
            "--set", "topology.worker_concurrency=8",
        ]))

        self.assertEqual(resolved["topology"]["worker_concurrency"], 8)

    def test_rejects_non_positive_worker_concurrency(self) -> None:
        args = parse_args([
            "run", "--scenario", "ready-success-v1", "--profile", "local-baseline",
            "--set", "topology.worker_concurrency=0",
        ])

        with self.assertRaisesRegex(ValueError, "worker_concurrency must be positive"):
            resolve_run(args)

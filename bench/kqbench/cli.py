"""kq-bench command-line entrypoint."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import resolve_run, resolve_sweep_runs
from .scenarios import load_sweep


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="kq-bench")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="run a harness scenario")
    run.add_argument("--scenario", help="catalogue scenario ID")
    run.add_argument("--config", help="ad hoc workload config path")
    run.add_argument("--profile", required=True, help="scale profile ID")
    run.add_argument(
        "--output",
        default="artifacts/performance",
        help="output directory for run artifacts",
    )
    run.add_argument(
        "--set",
        action="append",
        default=[],
        help="override resolved config (dotted key=value, repeatable)",
    )

    # A profile is selected at invocation so one sweep can exercise different
    # topologies without changing its rate progression.
    sweep = subparsers.add_parser("sweep", help="run an RPS sweep")
    sweep.add_argument("--name", required=True, help="catalogue sweep ID")
    sweep.add_argument("--profile", required=True, help="scale profile ID")
    sweep.add_argument(
        "--output",
        default="artifacts/performance",
        help="output directory for sweep and run artifacts",
    )
    sweep.add_argument(
        "--set",
        action="append",
        default=[],
        help="override each resolved run (dotted key=value, repeatable)",
    )

    args = parser.parse_args(argv)
    args.argv = ["kq-bench", *(argv if argv is not None else sys.argv[1:])]
    return args


def main() -> None:
    args = parse_args()
    if args.command == "run":
        resolved = resolve_run(args)
        # Imported late so configuration can be tested without orchestration.
        from .run import run_harness

        raise SystemExit(run_harness(resolved, Path(args.output)))
    if args.command == "sweep":
        definition = load_sweep(args.name)
        resolved = resolve_sweep_runs(definition, args)
        from .run import run_sweep

        raise SystemExit(run_sweep(definition, resolved, Path(args.output)))
    raise SystemExit(f"unknown command {args.command}")

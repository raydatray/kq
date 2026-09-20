# Performance Artifacts

This directory stores reproducible performance evidence separately from
implementation plans and recaps.

```text
templates/   canonical formats for new artifacts
scenarios/   stable catalogue of versioned workload definitions
runs/        immutable output from individual harness runs
```

## Artifact Model

```mermaid
flowchart LR
    Plan[Harness plan] --> Run[Immutable run]
    Run --> Analysis[Run analysis]
    Prior[Prior runs] --> Analysis
    Analysis --> Recap[Harness recap and decisions]
```

A run records one harness execution. It normally references a catalogue
scenario, but may instead contain an ad hoc workload with `scenario` set to
`null`. Its machine-generated metadata and results must not be edited after
publication. AI-generated interpretation belongs in the run's `analysis.md`.

Comparisons reference immutable run IDs directly. They do not copy or rewrite
the raw results of prior runs.

## Run IDs

Run directories use:

```text
YYYY-MM-DD-<scenario>-<revision>
```

The scenario component is the catalogue ID or a short `adhoc-*` label.

Example:

```text
runs/2026-09-18-serial-success-054333d/
```

Catalogue runs record their scenario identity:

```json
{
  "scenario": {
    "id": "ready-success-v1",
    "version": 1
  }
}
```

Ad hoc runs record `"scenario": null`; their `purpose` and resolved workload
describe what was executed.

## Run Contents

```text
run.json       environment and workload inputs
result.json    machine-generated output
analysis.md    AI-generated observations and suggestions
```

Large logs, profiles, and traces should remain CI artifacts or external object
storage. The run metadata may link to them, but they should not be committed to
this directory by default.

## Generating Analysis

The analysis generator receives only `run.json`, `result.json`, and retained
diagnostics, and keeps evidence and recommendations distinct:

```text
Observation: p95 delivery latency rose from 20ms to 400ms at 500 tasks/s.
Interpretation: the serial worker remained continuously occupied.
Suggestion: measure bounded worker concurrency against the same workload.
```

The generated analysis must not present a hypothesis as a measured fact. It
must record environmental caveats that could invalidate comparisons.

The runner itself never writes `analysis.md`; point an agent at the run
directory and `templates/analysis.md` to produce it.

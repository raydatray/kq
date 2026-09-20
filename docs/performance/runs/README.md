# Performance Runs

Each child directory contains one immutable harness run using the naming format
documented in the parent [README](../README.md).

Create new run directories from the `run.json` and `result.json` produced by
`kq-bench run`, following the shapes in [`templates`](../templates). Do not
edit them after publishing a run. Write `analysis.md` by pointing an agent
at the run directory and [`templates/analysis.md`](../templates/analysis.md).

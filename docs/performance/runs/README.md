# Performance Runs

Each dated child directory contains immutable harness evidence using the naming
format documented in the parent [README](../README.md). `_templates` contains
canonical artifact shapes and is not a run.

Create new run directories from the `run.json` and `result.json` produced by
`kq-bench run`, following the shapes in [`_templates`](./_templates). Do not
edit them after publishing a run. Write `analysis.md` by pointing an agent
at the run directory and [`_templates/analysis.md`](./_templates/analysis.md).

# Data Engine Spec

## Purpose

Data Engine is a code-defined workflow runtime. A flow is the `Flow` object returned by `build()`. The desktop app is an operator surface for discovery, execution, status, logs, and output inspection.

## Core Concepts

- `Flow(group)`: immutable flow definition
- `watch(...)`: runtime trigger binding for manual, poll, or schedule execution
- `mirror(root=...)`: mirrored output namespace binding
- `step(fn, use=..., save_as=..., label=...)`: one generic runtime step
- `map(fn, use=..., save_as=..., label=...)`: one callable applied across the current batch
- `step_each(fn, use=..., save_as=..., label=...)`: readability-first alias for `map(...)`
- `preview(use=...)`: notebook-style helper that returns one named saved object or the final current value
- `FlowContext`: mutable per-run state shared across steps
- `max_parallel`: optional source-scoped concurrency limit for eligible watched flows, defaulting to `1`

## Flow Module Source Model

- Authored flow modules live in `workspaces/<workspace_id>/flow_modules/`
- Compiled runtime modules live in `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/`
- Each flow module exports:
  - optional `DESCRIPTION`
  - `build() -> Flow`

Authored flow sources are authoritative. Compiled modules are generated runtime output.
The flow-module filename is the flow identity; authored flow modules only need to supply `group`.

## Flow Contract

Flows are code-defined only. There is no per-flow TOML layer.

Example polling flow:

```python
from data_engine import Flow
import polars as pl


def read_claims(context):
    return pl.read_excel(context.source.path)


def keep_open(context):
    return context.current.filter(pl.col("status") == "OPEN")


def write_target(context):
    output = context.mirror.with_suffix(".parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Claims")
        .watch(
            mode="poll",
            source="../example_data/Input/claims_dated",
            interval="5s",
            extensions=[".xlsx", ".xlsm"],
            settle=1,
            max_parallel=4,
        )
        .mirror(root="../example_data/Output/example_poll")
        .step(read_claims, save_as="raw_df")
        .step(keep_open, use="raw_df", save_as="filtered_df")
        .step(write_target, use="filtered_df")
    )
```

Example scheduled batch flow with `map()`:

```python
from data_engine import Flow
import duckdb
import polars as pl


def read_claims(file_ref):
    return pl.read_excel(file_ref.path)


def combine_claims(context):
    return pl.concat(context.current, how="vertical_relaxed")


def build_summary(context):
    conn = duckdb.connect(context.mirror.file("analytics.duckdb"))
    try:
        conn.register("input", context.current)
        return conn.sql("select workflow, count(*) as row_count from input group by workflow").pl()
    finally:
        conn.close()


def write_summary(context):
    output = context.mirror.file("workflow_summary.parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Analytics")
        .watch(mode="schedule", run_as="batch", interval="15m", source="../example_data/Input/claims_flat")
        .mirror(root="../example_data/Output/example_summary")
        .collect([".xlsx"], save_as="claim_files")
        .map(read_claims, use="claim_files", save_as="claim_frames")
        .step(combine_claims, use="claim_frames", save_as="raw_df")
        .step(build_summary, use="raw_df", save_as="summary_df")
        .step(write_summary, use="summary_df")
    )
```

Path helper intent:

- `with_extension(...)` keeps the mirrored source path and changes the extension
- `with_suffix(...)` is the equivalent pathlib-style alias
- `file(...)` writes into the mirrored/source folder with a custom filename
- `namespaced_file(...)` writes under a source-stem namespace for multi-output cases

## Runtime Rules

- polling freshness compares the current source file signature against persisted runtime ledger state
- output files are author-facing artifacts, not the freshness checkpoint
- intermediate step outputs do not participate in freshness
- groups still coordinate at the group level, while eligible source-scoped watched flows may execute several source runs concurrently inside one flow via `watch(..., max_parallel=...)`
- `max_parallel=1` preserves the existing sequential behavior
- `max_parallel` applies to source-scoped watched runs such as `run_as="individual"` poll or schedule execution; it does not make one step callable internally multithreaded
- stop requests are cooperative
- engine stop preserves graceful-stop semantics:
  - already-started work is allowed to finish
  - queued-not-yet-started work must not begin after stop is requested
- manual graceful stop follows the same intent for the targeted manual run
- step callables use native libraries directly; Data Engine does not mirror Polars or DuckDB APIs
- `map(...)` raises immediately when the current batch is empty
- `preview(use="name")` executes only until the named `save_as="name"` object exists, then returns that object

## UI Scope

The UI:

- discovers flows from compiled flow modules
- shows titles, descriptions, path bindings, runtime mode, and ordered steps
- supports `Run Once`, `Start Engine`, `Stop Runtime`, `Stop Flow`, output inspection, and docs viewing
- treats immediate control state and persisted runtime history as related but distinct surfaces
- talks to the per-workspace daemon through the daemon-manager layer

The UI does not:

- author flow modules
- define flow behavior
- own runtime execution logic

## Workspace Layout

- `src/data_engine/`: runtime package and desktop UI
- `workspaces/<workspace_id>/flow_modules/`: authored flow modules (`.py` or `.ipynb`)
- `workspaces/<workspace_id>/.workspace_state/`: shared lease/checkpoint state for one workspace
- `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/`: compiled/importable flow modules
- `artifacts/runtime_state/<workspace_id>/`: generated local runtime ledger state
- `artifacts/documentation/_build/html/`: generated documentation output
- sibling `example_data` / `data2`: starter fixtures and starter outputs for local development

Machine-local workspace discovery settings now live in the app-local SQLite settings store rather than in a repo-local `config/workspaces.toml` file.

The sibling starter-data trees are starter content only. Real path bindings belong in each flow definition.

## Live Smoke Coverage

- `tests/daemon/test_live_runtime_suite.py` is the end-to-end live smoke entrypoint
- it generates temporary workspaces and temporary data roots from scratch
- it verifies both Python-authored and notebook-authored poll, schedule, and manual flow modules
- it verifies one-daemon-per-workspace, run/start/stop/shutdown, and lease cleanup

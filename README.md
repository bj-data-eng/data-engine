# Data Engine

Data Engine is a pre-alpha workflow runtime for file-driven jobs. A flow declares:

- a group
- an optional runtime trigger via `watch(...)`
- ordered generic `step(...)` callables

The runtime orchestrates source handling, scheduling, and mirrored output routing. Poll freshness is tracked in the runtime ledger rather than by comparing output mtimes. Step functions use native libraries directly, such as Polars for dataframe work and DuckDB for SQL work.

## Install

### Installer scripts

Use the installer that matches your environment:

- macOS: [INSTALL/INSTALL MAC.command](INSTALL/INSTALL%20MAC.command)
- Windows: [INSTALL/INSTALL WINDOWS.bat](INSTALL/INSTALL%20WINDOWS.bat)
- Windows VM / CPU-safe Polars test path: [INSTALL/INSTALL WINDOWS_VM.bat](INSTALL/INSTALL%20WINDOWS_VM.bat)

The macOS and standard Windows installers install the normal base runtime, which now includes Polars directly.

### Manual install

Polars is now part of the regular install:

```bash
python -m pip install -e ".[dev]"
```

Launch the GUI with:

```bash
python -m data_engine.ui.cli.app start gui
```

## Public API

```python
from data_engine import Flow, FlowContext, discover_flows, load_flow, run
```

## Headless CLI

Data Engine now ships with a headless CLI:

```bash
data-engine list
data-engine show example_summary
data-engine run --once example_summary
data-engine run
```

`data-engine run` starts the automated engine headlessly for discovered automated flows and keeps running until stopped. Use `--once` to force a single pass instead.

## Workspace Model

Data Engine discovers workspaces from a collection root resolved from:

- `DATA_ENGINE_WORKSPACE_COLLECTION_ROOT`, when explicitly set
- `DATA_ENGINE_WORKSPACE_ROOT`, when binding directly to one authored workspace
- otherwise the machine-local app settings store

Each immediate child folder containing `flow_modules/` is treated as a workspace, for example:

- `workspaces/example_workspace/flow_modules/`
- `workspaces/claims2/flow_modules/`

The app resolves per-workspace local artifacts under:

- `artifacts/workspace_cache/<workspace_id>/`
- `artifacts/runtime_state/<workspace_id>/`

Shared lease and checkpoint state lives inside each authored workspace:

- `workspaces/<workspace_id>/.workspace_state/`

The app's workspace selection and collection-root preference are machine-local state, not repo-local config checked into the project tree.

## Basic shape

```python
from data_engine import Flow
import polars as pl


def read_claims(context):
    return pl.read_excel(context.source.path)


def keep_open(context):
    return context.current.filter(pl.col("status") == "OPEN")


def write_parquet(context):
    output = context.mirror.with_suffix(".parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Claims")
        .watch(
            mode="poll",
            source="../../../example_data/Input/claims_flat",
            interval="5s",
            extensions=[".xlsx", ".xls", ".xlsm"],
            settle=1,
        )
        .mirror(root="../../../example_data/Output/example_mirror")
        .step(read_claims, save_as="raw_df")
        .step(keep_open, use="raw_df", save_as="filtered_df")
        .step(write_parquet, use="filtered_df")
    )
```

## Batch helpers

For batch-oriented flows, use `Flow.collect(...)` and either `Flow.map(...)` or `Flow.step_each(...)` instead of importing extra helpers or hand-managing raw lists.

```python
from data_engine import Flow


def validate_workbook(context, file_ref):
    return {
        "name": file_ref.name,
        "path": file_ref.path,
        "ok": file_ref.exists(),
    }


def build():
    return (
        Flow(group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source="../../../example_data/Input/claims_flat")
        .collect([".xlsx"])
        .map(validate_workbook)
    )
```

`Flow.collect(...)` returns a `Batch` of `FileRef` items. `Flow.map(...)` applies one callable to each item and returns a new `Batch`. `Flow.step_each(...)` is the equivalent readability-first alias. If the batch is empty, both forms raise immediately so the mapped step gets the useful failure.

## Flow API

- `Flow(group=...)`
- `.watch(mode="manual", source=None, run_as="individual")`
- `.watch(mode="poll", source=..., interval=..., extensions=None, settle=1, run_as="individual")`
- `.watch(mode="schedule", interval=..., source=None, run_as="individual" | "batch")`
- `.watch(mode="schedule", time="HH:MM", source=None, run_as="individual" | "batch")`
- `.watch(mode="schedule", time=["08:15", "14:45"], source=..., run_as="individual" | "batch")`
- `.mirror(root=...)`
- `.step(fn, use=None, save_as=None, label=None)`
- `.collect(extensions, root=None, recursive=False, use=None, save_as=None, label=None)`
- `.map(fn, use=None, save_as=None, label=None)`
- `.step_each(fn, use=None, save_as=None, label=None)`
- `.preview(use=None)`
- `.run_once()`
- `.run()`
- `.show()`

`step()` callables always receive one `FlowContext` parameter and return the next value for `context.current`.
`map()` and `step_each()` callables accept either `(item)` or `(context, item)` and return a mapped `Batch`.

For notebook authoring, `preview()` is usually the most useful inspection helper:

```python
build().preview(use="raw_df").head(10)
build().preview(use="filtered_df")
```

`preview(use="name")` runs the flow until that `save_as="name"` object exists, then returns the real object without running later steps.

## Flow context

`FlowContext` exposes the active run state:

- `context.source`
- `context.mirror`
- `context.current`
- `context.objects`
- `context.metadata`
- `context.source_metadata()`

`context.source` is the resolved input namespace for the active source. The most useful helpers are:

- `context.source.path`
- `context.source.with_extension(".json")`
- `context.source.with_suffix(".json")`
- `context.source.file("notes.json")`
- `context.source.namespaced_file("notes.json")`
- `context.source.root_file("lookup.csv")`

`context.mirror` is the mirrored output namespace for the active source. The two core helpers are:

- `context.mirror.with_extension(".parquet")`
- `context.mirror.with_suffix(".parquet")`
- `context.mirror.file("open_claims.parquet")`
- `context.mirror.namespaced_file("open_claims.parquet")`

`with_extension(...)` is the clearer extension-changing helper. `with_suffix(...)` remains available as the pathlib-style alias.
`file(...)` stays in the mirrored/source folder. `namespaced_file(...)` creates a source-stem namespace for multi-output cases.

When a step writes one inspectable artifact, return that existing `Path`. The UI uses returned output paths to enable the `Inspect` button for that step.

`use="name"` loads `context.objects["name"]` into `context.current` before the step runs. `save_as="name"` stores the returned value into `context.objects["name"]`. Those same saved names are what `build().preview(use="name")` uses in notebooks.

## Discovery

Flows are code-defined. Starter flow modules live in:

- `workspaces/<workspace_id>/flow_modules/`
- `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/`

Each flow module must export:

- optional `DESCRIPTION`
- `build() -> Flow`

The flow-module filename is the flow identity. Authored flow modules should use `Flow(group=...)` and let the loader inject the name from the module filename.

Authored flow modules compile into `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/*.py`, and the runtime loads discovered flows from those compiled modules.

## Workspace layout

- `src/data_engine/`
  Runtime package and desktop UI
- `workspaces/<workspace_id>/flow_modules/`
  Authored flow sources (`.py` or `.ipynb`)
- `workspaces/<workspace_id>/.workspace_state/`
  Shared lease markers and checkpoint parquet snapshots
- `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/`
  Generated/importable flow modules
- `artifacts/runtime_state/<workspace_id>/`
  Internal runtime ledger state for one workspace
- `artifacts/documentation/`
  Generated documentation output
- `example_data/Input`
  Example input files
- `example_data/Settings`
  Example single-file inputs
- `example_data/Output`
  Flow outputs
- `example_data/databases`
  DuckDB files created on demand

## Live Smoke Suite

The live smoke suite is intentionally self-contained:

- `tests/daemon/test_live_runtime_suite.py`

It generates temporary workspaces from scratch, generates temporary `example_data/` and `data2/` with the real starter-data generator, adds notebook-authored poll/schedule/manual flows, runs the daemons, and tears the whole environment down afterward. It does not rely on existing `workspaces/example_workspace` or `workspaces/claims2` or live repo data directories.

## Status

This project is pre-alpha. Backwards compatibility is not a goal; the API should stay small and explicit while the runtime architecture settles.

# Data Engine

Data Engine is a GUI orchestrator for Python-based dataframe transform pipelines.

It provides:

- a workspace-based runtime for authored flows
- a desktop GUI for operators
- a terminal UI for headless/local operation
- an experimental Rust-backed `egui` surface
- parquet-first inspect/debug tooling for dataframe outputs

Flows are plain Python modules that declare how source files, settings workbooks, schedules, and manual runs should move through Polars, DuckDB, and file outputs.

## What It Is

Data Engine is not just a dataframe library and not just a scheduler. It is the operator/runtime layer around Python-authored flow modules.

The package handles:

- workspace discovery and selection
- daemon ownership and control handoff
- manual, poll, and schedule execution modes
- mirrored output routing
- persisted run/log/state history
- dataframe inspection inside the app

Step functions use normal Python libraries directly. In practice that usually means:

- Polars for dataframe transforms
- DuckDB for SQL-oriented work
- pathlib-style file output

## Install

### Installer scripts

Use the installer that matches your environment:

- macOS: [INSTALL/INSTALL MAC.command](INSTALL/INSTALL%20MAC.command)
- Windows: [INSTALL/INSTALL WINDOWS.bat](INSTALL/INSTALL%20WINDOWS.bat)
- Windows VM / CPU-safe Polars path: [INSTALL/INSTALL WINDOWS_VM.bat](INSTALL/INSTALL%20WINDOWS_VM.bat)

### Manual install

Base install:

```bash
python -m pip install py-data-engine
```

Editable local install:

```bash
python -m pip install -e .
```

Notebook-authored flow modules (`.ipynb`) are supported in the normal install. The optional notebook extra is only for authoring inside Jupyter:

```bash
python -m pip install -e ".[notebook]"
```

For contributors:

```bash
python -m pip install -e ".[dev]"
```

Core requirements:

- Python `>=3.14`
- PySide6 for the desktop GUI
- Textual for the terminal UI

## Start The App

Desktop GUI:

```bash
data-engine start gui
```

Experimental Rust `egui` surface:

```bash
data-engine start egui
```

Terminal UI:

```bash
data-engine start tui
```

You can also launch from module form if needed:

```bash
python -m data_engine.ui.cli.app start gui
```

## Headless CLI

```bash
data-engine list
data-engine show example_summary
data-engine run --once example_summary
data-engine run
```

`data-engine run` starts the automated engine headlessly for discovered automated flows and keeps running until stopped. Use `--once` to force a single pass instead.

## Public API

```python
from data_engine import Flow, FlowContext, discover_flows, load_flow, run
```

## Workspace Model

Data Engine discovers workspaces from a collection root resolved from:

- `DATA_ENGINE_WORKSPACE_COLLECTION_ROOT`, when explicitly set
- `DATA_ENGINE_WORKSPACE_ROOT`, when binding directly to one authored workspace
- otherwise the machine-local app settings store

Each immediate child folder containing `flow_modules/` is treated as a workspace, for example:

- `workspaces/example_workspace/flow_modules/`
- `workspaces/claims2/flow_modules/`

Shared workspace state lives inside each authored workspace:

- `workspaces/<workspace_id>/.workspace_state/`

Machine-local runtime state lives under the app artifacts root:

- `artifacts/workspace_cache/<workspace_id>/`
- `artifacts/runtime_state/<workspace_id>/`

The app's selected workspace and collection-root preference are machine-local settings, not repo-local config.

## Flow Shape

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

Each flow module exports:

- optional `DESCRIPTION`
- `build() -> Flow`

The module filename is the flow identity. Authored flow modules should set `Flow(group=...)` and let the loader inject the final flow name from the module filename.

## Runtime Modes

Flows can run as:

- `manual`
- `poll`
- `schedule`

At a high level:

- `manual` runs on operator request
- `poll` watches source inputs for new or changed files
- `schedule` runs on a time-based cadence

## Batch Helpers

For batch-oriented flows, use `Flow.collect(...)` plus either `Flow.map(...)` or `Flow.step_each(...)`.

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

`Flow.collect(...)` returns a `Batch` of `FileRef` items. `Flow.map(...)` applies one callable to each item and returns a new `Batch`. `Flow.step_each(...)` is the equivalent readability-first alias.

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

## FlowContext

`FlowContext` exposes the active run state:

- `context.source`
- `context.mirror`
- `context.current`
- `context.objects`
- `context.metadata`
- `context.source_metadata()`
- `context.debug`

Useful source helpers:

- `context.source.path`
- `context.source.with_extension(".json")`
- `context.source.with_suffix(".json")`
- `context.source.file("notes.json")`
- `context.source.namespaced_file("notes.json")`
- `context.source.root_file("lookup.csv")`

Useful mirror helpers:

- `context.mirror.with_extension(".parquet")`
- `context.mirror.with_suffix(".parquet")`
- `context.mirror.file("open_claims.parquet")`
- `context.mirror.namespaced_file("open_claims.parquet")`

`use="name"` loads `context.objects["name"]` into `context.current` before the step runs. `save_as="name"` stores the returned value into `context.objects["name"]`.

## Dataframe Debugging

The app includes a dataframe-first debug pane for saved parquet artifacts.

From a flow step, save a debug dataframe with:

```python
context.debug.save_frame(context.current, name="raw_claims")
```

That writes:

- a parquet artifact for the dataframe
- companion metadata used by the UI

The desktop GUI can then:

- list saved dataframe artifacts by flow/step/timestamp
- inspect parquet outputs in-app
- preview top N, bottom N, or sampled rows
- filter columns with Excel-style distinct-value popups
- copy one or more selected cells from the table

The inspect modal reuses the same dataframe rendering path.

## Notebook Preview

For notebook authoring, `preview()` is usually the most useful helper:

```python
build().preview(use="raw_df").head(10)
build().preview(use="filtered_df")
```

`preview(use="name")` runs the flow until that `save_as="name"` object exists, then returns the real object without running later steps.

## Discovery And Compilation

Starter flow modules live in:

- `workspaces/<workspace_id>/flow_modules/`
- `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/`

Authored flow modules compile into:

- `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/*.py`

The runtime loads discovered flows from those compiled modules.

## Workspace Layout

- `src/data_engine/`
  Runtime package, operator surfaces, and services
- `workspaces/<workspace_id>/flow_modules/`
  Authored flow sources (`.py` or `.ipynb`)
- `workspaces/<workspace_id>/.workspace_state/`
  Shared lease markers and checkpoint parquet snapshots
- `artifacts/workspace_cache/<workspace_id>/compiled_flow_modules/`
  Generated importable flow modules
- `artifacts/runtime_state/<workspace_id>/`
  Machine-local runtime and daemon state
- `src/data_engine/docs/`
  Packaged documentation content

## Smoke Data

Generate local smoke data with:

```bash
python scripts/generate_smoke_data.py --root . --workspace-id example_workspace --workspace-id claims2
```

Generated local data and workspaces are intentionally ignored:

- `data/`
- `data2/`
- `workspaces/`

## Packaging

Distribution name:

- `py-data-engine`

Version source of truth:

- `src/data_engine/platform/identity.py`

Build checks:

```bash
python -m build
python -m twine check dist/*
```

## Status

This project is pre-alpha. Internal architecture is still moving quickly, and backwards compatibility is not a current goal.

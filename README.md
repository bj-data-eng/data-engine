# Data Engine

Data Engine is a GUI orchestrator for Python dataframe workflows. It lets you
author flows as plain Python modules, run them manually or automatically, and
inspect parquet-first outputs from the desktop app.

The runtime is built around:

- workspace-based flow discovery
- manual, poll, and schedule execution modes
- Polars and DuckDB-friendly flow steps
- mirrored output paths for source-driven runs
- saved run, log, and dataframe inspection state
- a desktop operator surface

## Install

Use the installer for your environment:

- macOS: [INSTALL/INSTALL MAC.command](INSTALL/INSTALL%20MAC.command)
- Windows: [INSTALL/INSTALL WINDOWS.bat](INSTALL/INSTALL%20WINDOWS.bat)
- Windows VM / CPU-safe Polars path: [INSTALL/INSTALL WINDOWS_VM.bat](INSTALL/INSTALL%20WINDOWS_VM.bat)

For local development:

```bash
python -m pip install --constraint requirements/constraints.txt -e ".[dev]"
```

For a published package install:

```bash
python -m pip install py-data-engine
```

Data Engine requires Python `>=3.14`.

Dependency pinning, constrained installs, and hash-locked runtime installs are
documented in [SECURITY.md](SECURITY.md).

## Start

Desktop GUI:

```bash
data-engine start gui
```

Headless commands:

```bash
data-engine list
data-engine show example_summary
data-engine run --once example_summary
data-engine run
```

## Minimal Flow

```python
from data_engine import Flow
import polars as pl


def read_docs(context):
    return pl.read_excel(context.source.path)


def keep_open(context):
    return context.current.filter(pl.col("status") == "OPEN")


def write_parquet(context):
    output = context.mirror.with_suffix(".parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Docs")
        .watch(
            mode="poll",
            source="../../../example_data/Input/docs_flat",
            interval="5s",
            extensions=[".xlsx", ".xls", ".xlsm"],
            settle=1,
        )
        .mirror(root="../../../example_data/Output/example_mirror")
        .step(read_docs, save_as="raw_df")
        .step(keep_open, use="raw_df", save_as="filtered_df")
        .step(write_parquet, use="filtered_df")
    )
```

Each authored flow module exports `build() -> Flow`. The module filename is the
flow identity.

## Workspaces

Data Engine discovers workspaces from a collection root. Each workspace keeps
authored flows under:

```text
workspaces/<workspace_id>/flow_modules/
```

Shared workspace state lives under:

```text
workspaces/<workspace_id>/.workspace_state/
```

Machine-local runtime artifacts are stored outside the authored workspace.

## Useful APIs

```python
from data_engine import Flow, FlowContext, discover_flows, load_flow, run
```

Common `Flow` methods:

- `.watch(...)`
- `.mirror(...)`
- `.date_range_input(...)`
- `.step(...)`
- `.collect(...)`
- `.map(...)`
- `.step_each(...)`
- `.preview(...)`
- `.run_once()`
- `.run()`

Common `FlowContext` values:

- `context.source`
- `context.mirror`
- `context.current`
- `context.objects`
- `context.metadata`
- `context.database("analytics.duckdb")`
- `context.template("reports/base.xlsx")`
- `context.debug`

The full authoring guide and helper reference live in
`src/data_engine/docs/sphinx_source/guides/`.

## Testing

```bash
python -m pytest -q
python -m build
python -m twine check dist/*
```

## Status

This project is pre-alpha. Internal architecture is still moving quickly, and
backwards compatibility is not a current goal.

# Authoring Flow Modules

Flow modules live in:

- `workspaces/<workspace_id>/flow_modules/<name>.ipynb`
- `workspaces/<workspace_id>/flow_modules/<name>.py`

Reusable helper modules can live in:

- `workspaces/<workspace_id>/flow_modules/flow_helpers/<name>.py`

Each flow module should export:

- optional `DESCRIPTION`
- `build() -> Flow`

The flow-module filename is the durable flow identity used by discovery and runtime state. If you rename the file, you are effectively creating a different flow as far as the system is concerned.

## Required contract

```python
from data_engine import Flow

DESCRIPTION = "Reads workbook inputs and writes mirrored parquet outputs."


def build():
    return Flow(group="Claims")
```

When you want a custom display title in the UI, set `label=` on the returned `Flow(...)`. Otherwise the UI derives a readable title from the flow-module filename.

`build()` must not accept any parameters.

Keep module import-time code side-effect free. The app needs to discover flows safely and repeatedly, so top-level code should not:

- run queries
- write files
- start background work
- depend on interactive state

Do that work inside steps instead.

## Step style

Every `step(...)` callable receives one `context` argument:

```python
def read_claims(context):
    ...


def clean_claims(context):
    ...
```

`map(...)` and `step_each(...)` are the batch-oriented exception. They accept either:

```python
def validate_pdf(file_ref):
    ...


def validate_pdf_with_context(context, file_ref):
    ...
```

`map(...)` always returns a `Batch`, and `step_each(...)` is the equivalent alias. Both raise immediately when the current batch is empty.

Use native libraries directly inside those steps:

- Polars for dataframe reads, transforms, and writes
- DuckDB for SQL and database work
- `pathlib` and normal Python for filesystem logic

That simplicity is the intended authoring experience. Flow modules should feel like normal Python modules with a small orchestration surface.

## Good patterns

- keep import-time code side-effect free
- keep expensive work inside steps
- use `save_as=` and `use=` to preserve intermediate objects
- use `build().preview(use="name")` in notebooks when you want to inspect one saved intermediate object quickly
- use `collect(...)` when you want a batch of files
- use `map(...)` or `step_each(...)` when the same callable should run once per batch item
- use `context.source` for source-relative paths
- use `context.mirror` for write-ready output paths
- return the written `Path` from output steps so the UI can enable `Inspect`
- move shared parsing, SQL, and utility code into `flow_modules/flow_helpers/*.py` and import it from flows with `from flow_helpers.<name> import ...`

Also good:

- use `context.config.require("name")` for required TOML config
- use `context.database("analytics/db.duckdb")` for workspace-local database paths
- record useful UI/runtime details in `context.metadata`
- keep writer steps narrow and explicit
- split "build data" and "write data" into separate steps when you want a previewable intermediate

Usually worth avoiding:

- monolithic steps that read, transform, and write everything at once
- hand-built relative path logic when `context.source` or `context.mirror` already models it
- hidden global state in helper modules
- returning a path that was never actually written

## Helper modules

Helper modules are regular Python files under `flow_modules/flow_helpers/`. They are compiled into workspace-local runtime artifacts and are importable from both notebook-authored and Python-authored flows.

Example:

```python
# flow_modules/flow_helpers/claims_sql.py
LATEST_CLAIMS_SQL = "select * from claims where is_latest = true"
```

```python
# flow_modules/claims_report.py
from flow_helpers.claims_sql import LATEST_CLAIMS_SQL
from data_engine import Flow


def build():
    return Flow(group="Claims")
```

Files in `flow_modules/flow_helpers/` support authored flow modules and stay out of runnable flow discovery.

Helper imports are resolved against the currently selected workspace during flow loading. That isolation matters when two workspaces use the same helper module names, because one workspace's helper cache should never leak into another workspace's flow import.

Flow-module compilation is content-aware. If you save a source file twice in quick succession on a filesystem with coarse mtimes, Data Engine still recompiles when the rendered module text changed.

This is the right home for:

- shared SQL strings
- parsing helpers
- file naming utilities
- common dataframe transforms
- shared constants

Code that should run independently and appear in the app belongs in its own flow module with its own `build()`.

## Example

```python
from data_engine import Flow
import polars as pl


def read_claims(file_ref):
    return pl.read_excel(file_ref.path)


def concat_claims(context):
    return pl.concat(context.current, how="vertical_relaxed")


def keep_completed(context):
    return context.current.filter(pl.col("Step TO") == "COMPLETED")


def write_target(context):
    output = context.mirror.file("example_completed.parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/claims_flat")
        .mirror(root="../../example_data/Output/example_completed")
        .collect([".xlsx"], save_as="claim_files")
        .map(read_claims, use="claim_files", save_as="claim_frames")
        .step(concat_claims, use="claim_frames", save_as="raw_df")
        .step(keep_completed, use="raw_df", save_as="clean_df")
        .step(write_target, use="clean_df")
    )
```

That example shows `map(...)` in context:

- `collect(...)` gathers a batch of `FileRef` items
- `map(...)` reads each file into one dataframe
- later `step(...)` callables operate on the whole batch result

There is no separate config layer that turns one flow module into multiple named flow variants after build time.

## Notebook-authored vs Python-authored modules

Both notebook and Python flow modules participate in the same discovery model:

- they export one `build() -> Flow`
- they can import helper modules
- they compile into runtime-ready Python modules

Python modules are usually better for:

- shared flows
- helper-heavy logic
- larger code review surfaces

Notebooks are usually better for:

- exploratory authoring
- iterative preview-driven development
- flows that benefit from inline inspection while being built

## A practical authoring checklist

Before calling a flow module "done," it is worth checking:

- `build()` returns one `Flow`
- the module imports cleanly with no side effects
- the step labels are readable in the UI
- saved object names are meaningful
- required config is documented or obvious
- writer steps return actual existing paths when you want inspectability
- any helper modules sit under `flow_modules/flow_helpers/`

# Getting Started

This guide is for someone new to the code-defined Data Engine API and desktop app.

By the end, you should understand:

- what a flow is
- where flow modules live
- what a workspace contains
- how discovery and runtime execution work at a high level
- how to run a first flow end to end
- how batch workflows fit into the model

## The mental model

Data Engine has one source of truth for per-flow behavior: the `Flow` returned by `build()`.

In practice:

- the flow module defines the flow name, group, runtime mode, and ordered steps
- step functions do real work with native libraries such as Polars, DuckDB, and plain Python
- the desktop app discovers those flow modules inside the selected workspace and shows them as configurable runnable flows

The fluent API owns orchestration, while the step callables own your actual business logic.

## The basic workspace layout

A typical authored workspace looks like this:

```text
workspaces/
  example_workspace/
    flow_modules/
    flow_modules/flow_helpers/
    config/
    databases/
    .workspace_state/
```

The parts you will usually author directly are:

- `flow_modules/`: runnable flows in `.py` or `.ipynb`
- `flow_modules/flow_helpers/`: reusable helper modules imported from flows
- `config/`: workspace-local TOML files available through `context.config`
- `databases/`: a conventional home for workspace-local databases used through `context.database(...)`

The app can provision that shape for you without overwriting existing content.

## Where flow module sources live

Flow module sources are authored in:

- `workspaces/<workspace_id>/flow_modules/<name>.ipynb`
- `workspaces/<workspace_id>/flow_modules/<name>.py`

Reusable helper modules live in:

- `workspaces/<workspace_id>/flow_modules/flow_helpers/<name>.py`

Compiled runtime modules are generated into machine-local artifacts.
Those runtime artifacts are isolated per workspace, so helper imports with the same module names stay workspace-local.

Each flow module should export:

- optional `DESCRIPTION`
- `build() -> Flow`

Display titles come from `Flow(label=...)` when provided. Otherwise the UI derives them from the flow-module filename.

## Your first flow

A minimal scheduled flow can create data in memory and write it out:

```python
from data_engine import Flow
import polars as pl


def build_dates(context):
    return pl.DataFrame({"day": [1, 2, 3]})


def write_dates(context):
    output = context.mirror.file("dates.parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Reference")
        .watch(mode="schedule", run_as="batch", interval="1h")
        .mirror(root="../../example_data/Output/date_dimension")
        .step(build_dates, save_as="dates_df")
        .step(write_dates, use="dates_df", label="Write Parquet")
    )
```

That example shows the full shape:

1. create `Flow(group=...)`
2. attach a runtime mode with `watch(...)`
3. optionally attach `mirror(...)`
4. add ordered `step(...)` callables
5. return the built flow from `build()`

The return value from each step becomes `context.current`, so later steps can keep operating on the current object or reach back to previously saved objects through `use=`.

## What the app actually does with that flow

Once the flow is discovered, the desktop app uses it for:

- grouping and labels in the home view
- deciding whether the flow is manual, poll, or schedule
- deciding whether the flow participates in the engine
- rendering step names and inspectable outputs
- manual runs and engine runs for the selected workspace

The app itself binds to one workspace at a time, so when you switch workspaces, the discovered flows, runtime ledger, daemon state, and visible runs all switch with it.

## A starter-style polling flow

This shape maps directly to starter flows such as `example_mirror` and `example_poll`:

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
            source="../../example_data/Input/claims_dated",
            interval="5s",
            extensions=[".xlsx", ".xlsm"],
            settle=1,
        )
        .mirror(root="../../example_data/Output/example_poll")
        .step(read_claims, save_as="raw_df")
        .step(keep_open, use="raw_df", save_as="filtered_df")
        .step(write_target, use="filtered_df", label="Write Parquet")
    )
```

This is a good first mental model for source-driven flows:

- `watch(...)` tells the runtime what to listen to
- `context.source` tells the step which concrete file is active
- `mirror(...)` defines where mirrored outputs belong
- returning the written path makes the result inspectable in the UI

## Batch-oriented files

When you want a folder of files as one runtime object, use `Flow.collect(...)` and either `Flow.map(...)` or `Flow.step_each(...)`.

```python
from data_engine import Flow


def validate_pdf(file_ref):
    return {"name": file_ref.name, "ok": file_ref.exists()}


def summarize_results(context):
    return tuple(item["name"] for item in context.current if item["ok"])


def build():
    return (
        Flow(group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/pdfs")
        .collect(extensions=[".pdf"], save_as="pdf_files")
        .map(fn=validate_pdf, use="pdf_files", save_as="pdf_results")
        .step(summarize_results, use="pdf_results")
    )
```

`Flow.collect(...)` returns a `Batch` of `FileRef` items.

`Flow.map(...)` runs one callable per item and returns a new `Batch`.

`Flow.step_each(...)` is the same operation with a name that can read more clearly in some flows.

If the batch is empty, both forms raise immediately. That behavior makes batch-flow outcomes explicit and easy to diagnose.

## Running flows from Python

Load one discovered flow:

```python
from data_engine import load_flow

built = load_flow("example_poll")
results = built.run_once()
```

Discover everything the workspace exposes:

```python
from data_engine import discover_flows, run

flows = discover_flows()
run(*flows)
```

Notebook-authored flows also support preview-oriented authoring:

```python
build().preview()
build().preview(use="raw_df")
```

That is often the fastest way to sanity-check a flow while you are still writing it.

For poll flows that watch a folder, `preview(...)` uses one deterministic startup source as a representative notebook preview.

## Manual, poll, and schedule at a glance

### Manual

- `watch(mode="manual")`
- `context.current` starts as `None`
- useful for ad hoc or UI-driven runs
- works well for flows that build data in memory or start from operator actions

### Poll

- `watch(mode="poll", ...)`
- watches either one file or a directory of source files
- the first step receives the active source through `context.source`
- freshness compares the current source file signature against the runtime ledger
- `extensions=` and `settle=` only apply here

### Schedule

- `watch(mode="schedule", ...)`
- runs on an interval or on one or more wall-clock times
- supports one `time="HH:MM"` value or a collection of times
- often starts by building data in memory or loading from a known source root

## A few good habits early

- keep import-time code side-effect free
- keep expensive work inside steps
- return output paths from writer steps when you want the UI `Inspect` action
- move reusable SQL, parsing helpers, and constants into `flow_modules/flow_helpers/`
- use `context.config` for workspace-local TOML configuration
- use `context.database(...)` when you want a conventional workspace-local database path

## Next steps

- Read [Core Concepts](core-concepts.md)
- Read [Authoring Flow Modules](authoring-flow-modules.md)
- Read [Flow Methods](flow-methods.md)
- Read [FlowContext](flow-context.md)
- Read [App Runtime and Workspaces](app-runtime-and-workspaces.md)

# Core Concepts

## Flow

A `Flow` is an immutable definition with:

- `group`
- an optional trigger via `watch(...)`
- an optional mirrored output binding via `mirror(...)`
- ordered generic steps

```python
from data_engine import Flow

flow = Flow(group="Claims")
```

The flow-module filename is the flow identity used for discovery and runtime bookkeeping. `group` is the author-controlled grouping visible in the UI.

## Runtime modes

Manual:

- no trigger configured
- `run_once()` executes the steps once with `context.current = None`
- useful for button-driven operator runs or preview-oriented flows

Poll:

- source-driven execution over either one file or a directory of files
- the runtime compares the current source file signature against the persisted runtime ledger
- the first step sees the active input through `context.source`
- startup backlog handling is based on persisted ledger state for each source version
- intermediate saved objects do not participate in staleness checks

Schedule:

- interval-driven via `watch(mode="schedule", interval="15m")`
- or wall-clock via `watch(mode="schedule", time="10:31")`
- `time` may also be a collection such as `["08:15", "14:45"]`
- may optionally bind a `source=...` path for recurring jobs

The distinction between poll and schedule is important:

- poll is source freshness driven
- schedule is time driven

You can combine scheduled execution with a source binding when the flow should run on a schedule but still read from a known source root.

## Step

Each `step(...)` is one callable:

```python
def step(context) -> object:
    ...
```

The return value always becomes `context.current`.

This is the main design boundary:

- the fluent API orchestrates runtime behavior
- native libraries perform the actual data and file work

That means Data Engine coordinates Polars, DuckDB, pathlib, and your Python helper code through one runtime model.

## Saved objects

Steps can save and reuse values:

```python
(
    Flow(group="Docs")
    .step(read_claims, save_as="raw_df")
    .step(clean_claims, use="raw_df", save_as="clean_df")
    .step(write_output, use="clean_df")
)
```

- `use="name"` loads `context.objects["name"]` into `context.current`
- `save_as="name"` stores the returned value into `context.objects["name"]`

In notebooks, those saved names are also the easiest way to inspect intermediates:

```python
build().preview(use="clean_df").head(10)
```

This is one of the most useful parts of the authoring model:

- `current` gives you the current object in the pipeline
- `objects` gives you stable named waypoints

That makes it easy to structure flows around a few explicit intermediate states and readable named waypoints.

## Batch mapping

`collect(...)` and `map(...)` are the batch-oriented authoring tools.

```python
def read_claims(file_ref):
    return pl.read_excel(file_ref.path)


def combine_claims(context):
    return pl.concat(context.current, how="vertical_relaxed")


flow = (
    Flow(group="Analytics")
    .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/claims_flat")
    .collect([".xlsx"], save_as="claim_files")
    .map(read_claims, use="claim_files", save_as="claim_frames")
    .step(combine_claims, use="claim_frames")
)
```

Use `map(...)` when the same callable should run once per batch item. `map(...)` raises immediately when the batch is empty.

Batch mapping is especially useful when you want to:

- read many files into many dataframes
- validate one file at a time
- emit one lightweight record per source item before combining

Use a normal `step(...)` when the callable should reason about the batch as a whole.

## Source and mirror namespaces

The runtime exposes two structured path namespaces:

- `context.source`
- `context.mirror`

Examples:

```python
context.source.path
context.source.with_extension(".json")
context.source.with_suffix(".json")
context.source.file("notes.json")
context.source.namespaced_file("notes.json")
context.source.root_file("lookup.csv")

context.mirror.with_extension(".parquet")
context.mirror.with_suffix(".parquet")
context.mirror.file("open_claims.parquet")
context.mirror.namespaced_file("open_claims.parquet")
context.mirror.root_file("analytics.duckdb")
```

`context.source` resolves read-side paths. `context.mirror` resolves write-ready output paths.

The important difference is:

- `source` is about where the active input lives
- `mirror` is about where outputs for that input should go

That lets you keep path logic readable and source-aware without hand-building relative paths in every step.

Examples of common patterns:

- read a sidecar file beside the current source with `context.source.file("notes.json")`
- write one mirrored parquet beside the source shape with `context.mirror.with_suffix(".parquet")`
- write multiple outputs for the same source with `context.mirror.namespaced_file(...)`
- write a stable root-level artifact such as a snapshot or DuckDB file with `context.mirror.root_file(...)`

## Discovery

The desktop UI and Python entrypoints discover flows from compiled flow modules.

Each discovered flow module contributes:

- a module name
- optional `DESCRIPTION`
- `build() -> Flow`

The flow-module filename/module name is the flow identity surfaced in discovery and execution. The UI uses `Flow.label` when present, otherwise it derives a readable title from that internal name.

That discovered `Flow` object is what the UI inspects for:

- grouping
- step labels
- runtime mode
- source and mirror bindings

The authored `Flow` is the contract the runtime and UI inspect after discovery.

## Workspaces

Flows are discovered from the currently selected authored workspace.

An authored workspace typically contains:

- `flow_modules/`
- `flow_modules/flow_helpers/`
- `config/`
- `databases/`

The desktop app binds to one workspace at a time. When the selected workspace changes, the app reloads:

- discovered flows
- local runtime state
- daemon control state
- visible runs and logs

For the control and state model behind that, see [App Runtime and Workspaces](app-runtime-and-workspaces.md).

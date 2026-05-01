# Core Concepts

## Flow

A `Flow` is an immutable definition with:

- `group`
- optional `name` and `label`
- an optional trigger via `watch(...)`
- an optional mirrored output binding via `mirror(...)`
- ordered generic steps

```python
from data_engine import Flow

flow = Flow(group="Docs", label="Open Docs")
```

Builder methods return a new `Flow` instead of mutating the existing object:

```python
base = Flow(group="Docs")
manual_flow = base.watch(mode="manual")
```

The flow-module filename is the flow identity used for discovery and runtime bookkeeping. In normal flow modules, let the loader derive `name` from that filename. `group` controls the UI grouping, and `label` optionally overrides the display title.

If a flow has no trigger, its effective mode is manual.

## Runtime modes

Manual:

- no trigger configured, or `watch(mode="manual")`
- `run_once()` executes the steps once with `context.current = None`
- `watch(mode="manual")` may still carry a source binding, `run_as`, and `max_parallel`
- useful for button-driven operator runs or preview-oriented flows

Poll:

- source-driven execution over either one file or a directory of files
- the runtime compares the current source file signature against the persisted runtime ledger
- the first step sees the active input through `context.source`
- startup backlog handling is based on persisted ledger state for each source version
- intermediate saved objects do not participate in staleness checks
- requires both `source=` and `interval=`
- accepts `extensions=` and `settle=` for source discovery and change stability

Schedule:

- interval-driven via `watch(mode="schedule", interval="15m")`
- or wall-clock via `watch(mode="schedule", time="10:31")`
- `time` may also be a collection such as `["08:15", "14:45"]`
- may optionally bind a `source=...` path for recurring jobs
- accepts exactly one of `interval=` or `time=`

The distinction between poll and schedule is important:

- poll is source freshness driven
- schedule is time driven

You can combine scheduled execution with a source binding when the flow should run on a schedule but still read from a known source root.

`run_as` controls what counts as one run:

- `run_as="individual"` runs once per concrete source file
- `run_as="batch"` runs once for the watched source root

`max_parallel` controls concurrent source-file runs for one flow when `run_as="individual"`. It defaults to `1`.

## Step

Each `step(...)` is one callable that accepts exactly one `FlowContext`:

```python
def step(context) -> object:
    ...
```

The return value always becomes `context.current`.

`use=` on a step loads a saved object from `context.objects` into `context.current` before the callable runs. `use="current"` leaves the current value in place. `save_as=` stores the returned value back into `context.objects` for later steps, previews, or notebook inspection. The runtime owns `current`, so `save_as="current"` is rejected.

`label=` overrides the step display name. When omitted, Data Engine derives the label from the callable name.

This is the main design boundary:

- the fluent API orchestrates runtime behavior
- native libraries perform the actual data and file work

That means Data Engine coordinates Polars, DuckDB, pathlib, and your Python helper code through one runtime model.

## Saved objects

Steps can save and reuse values:

```python
(
    Flow(group="Docs")
    .step(read_docs, save_as="raw_df")
    .step(clean_docs, use="raw_df", save_as="clean_df")
    .step(write_output, use="clean_df")
)
```

- `use="name"` loads `context.objects["name"]` into `context.current`
- `save_as="name"` stores the returned value into `context.objects["name"]`
- `use="current"` leaves the current runtime value in place
- `save_as="current"` is invalid

In an external notebook or REPL, those saved names are also the easiest way to inspect intermediates:

```python
build().preview(use="clean_df").head(10)
```

This is one of the most useful parts of the authoring model:

- `current` gives you the current object in the pipeline
- `objects` gives you stable named waypoints

That makes it easy to structure flows around a few explicit intermediate states and readable named waypoints.

## Batch mapping

`collect(...)`, `map(...)`, and `step_each(...)` are the batch-oriented authoring tools.

```python
def read_docs(file_ref):
    return pl.read_excel(file_ref.path)


def combine_docs(context):
    return pl.concat(context.current, how="vertical_relaxed")


flow = (
    Flow(group="Analytics")
    .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/docs_flat")
    .collect([".xlsx"], save_as="doc_files")
    .map(read_docs, use="doc_files", save_as="doc_frames")
    .step(combine_docs, use="doc_frames")
)
```

`collect(...)` gathers matching files into a `Batch` of `FileRef` items. `map(...)` runs the same callable once per batch item, and `step_each(...)` is the equivalent alias. `map(...)` accepts either `item` or `context, item` and raises immediately when the batch is empty or not iterable.

If `collect(root=...)` is omitted, collection uses the active source root from `context.source`. `recursive=True` searches child directories. A missing collection root returns an empty `Batch`; the following `map(...)` or `step_each(...)` then fails loudly instead of treating "no files" as success.

`Batch` behaves like a small immutable sequence:

```python
def summarize_results(context):
    files = context.current
    return {"count": len(files), "names": files.names()}
```

`FileRef` wraps a concrete path and exposes `path`, `name`, `stem`, `suffix`, `parent`, and `exists()`. It also works with APIs that accept filesystem paths.

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
context.mirror.file("open_docs.parquet")
context.mirror.namespaced_file("open_docs.parquet")
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

Helpers that depend on a concrete source file, such as `with_suffix(...)` and `namespaced_file(...)`, raise when the run only has a source root. That can happen in root-level batch runs.

## Flow context

Every step receives a `FlowContext`:

```python
def inspect_source(context):
    metadata = context.source_metadata()
    return {
        "flow": context.flow_name,
        "source": metadata.name if metadata else None,
        "objects": tuple(context.objects),
    }
```

The core fields are:

- `flow_name` and `group`: runtime identity and grouping
- `current`: the active pipeline value
- `objects`: named intermediate values saved by `save_as=`
- `metadata`: runtime annotations attached to the execution
- `source`: source path helpers when the run is source-backed
- `mirror`: output path helpers when the flow configured `mirror(root=...)`
- `config`: lazy workspace TOML reader
- `debug`: optional debug artifact writer for runtime diagnostics

`context.config` reads `<workspace>/config/*.toml` by stem:

```python
settings = context.config.require("settings")
```

Use `context.config.get(...)` for optional files, `require(...)` for required files, `names()` to list available config stems, and `all()` to load all visible config mappings.

`context.database("analytics.duckdb")` returns a write-ready path beneath the current workspace's `databases/` directory. It is intended for durable workspace-local database files.

When debug artifact capture is enabled by the runtime, `context.debug.save_frame(...)` can save a Polars frame and `context.debug.save_json(...)` can save a JSON artifact for inspection. Guard debug calls when the same flow should run with or without debug capture:

```python
if context.debug is not None:
    context.debug.save_json({"rows": len(context.current)}, name="row-count")
```

## Discovery

The desktop UI and Python entrypoints discover flows from compiled `.py` and `.ipynb` flow modules.

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

Notebook-authored flows are compiled into the same discovery surface as Python-authored flows, so both authoring styles participate in the same workspace layout and runtime rules.

The desktop app binds to one workspace at a time. When the selected workspace changes, the app reloads:

- discovered flows
- local runtime state
- daemon control state
- visible runs and logs

For the control and state model behind that, see [App Runtime and Workspaces](app-runtime-and-workspaces.md).

# FlowContext

`FlowContext` is the mutable runtime object passed to normal flow steps. It is
the main authoring surface for reading the active value, reusing named
intermediates, resolving source and output paths, loading workspace config, and
publishing runtime metadata.

Most steps only need one or two context features:

```python
def capture_row_count(context):
    context.metadata["row_count"] = len(context.current)
    return context.current
```

## Runtime fields

`FlowContext` exposes these fields:

- `flow_name`: stable flow name for the current execution
- `group`: flow group used by operator surfaces
- `source`: `SourceContext | None` for source-backed executions
- `mirror`: `MirrorContext | None` when the flow configured `mirror(root=...)`
- `current`: the active value moving through the pipeline
- `objects`: named intermediate values saved with `save_as=`
- `metadata`: free-form metadata attached to the execution
- `config`: lazy reader for workspace `config/*.toml`
- `debug`: optional debug artifact writer for the active run

The core model is small:

1. `current` is the value handed from step to step.
2. `objects` stores named values created by `save_as=`.
3. `source` and `mirror` are path namespaces, not open files or connections.

## `flow_name` and `group`

Use `flow_name` and `group` when a step needs to label outputs, metadata, logs,
or diagnostics with flow identity:

```python
def stamp_identity(context):
    context.metadata["flow"] = context.flow_name
    context.metadata["group"] = context.group
    return context.current
```

`flow_name` comes from the authored flow module identity. `group` comes from the
flow definition, such as `Flow(group="Documents")`.

## `current`

`context.current` is the active runtime slot:

- before the first manual or scheduled step, it is usually `None`
- after each step, it becomes that step's return value
- when a step declares `use=`, the runtime loads that named object into
  `current` before calling the step

```python
def keep_open_rows(context):
    frame = context.current
    return frame.filter(frame["status"] == "open")
```

## `objects`

`context.objects` is the dictionary behind `save_as=` and `use=`.

```python
(
    Flow(group="Documents")
    .step(read_input, save_as="raw")
    .step(clean_rows, use="raw", save_as="clean")
    .step(write_output, use="clean")
)
```

A step can also read multiple saved values directly:

```python
def compare_versions(context):
    previous = context.objects["previous"]
    current = context.objects["current"]
    return build_delta(previous, current)
```

Use direct `objects` access when a step needs more than the one active value
provided by `use=`.

## `metadata`

`context.metadata` is a free-form dictionary for execution annotations. It is a
good fit for row counts, selected config values, source details, warning flags,
and other lightweight operator diagnostics.

The runtime seeds execution metadata, including:

- `started_at_utc`
- `run_id`
- `step_outputs`
- `file_hash` when the run is bound to a concrete source file

`file_hash` is a stable SHA-1 hash of the source-relative path when one exists.
For single-file bindings, it falls back to the concrete source path text.

```python
def capture_stats(context):
    context.metadata["rows"] = len(context.current)
    context.metadata["source"] = (
        context.source.path.name if context.source and context.source.path else None
    )
    return context.current
```

When a step writes a file and returns an existing `Path`, the runtime records
that output path in metadata so operator surfaces can inspect it.

## `config`

`context.config` is a `WorkspaceConfigContext`. It lazily reads TOML files from
the authored workspace's `config/` directory and returns plain dictionaries.
It is read-only from the flow author's perspective.

Available helpers:

```python
context.config.config_dir
context.config.names()
context.config.get("runtime")
context.config.require("runtime")
context.config.all()
```

### `config_dir`

`config_dir` is the workspace `config/` path, or `None` when the context is not
attached to an authored workspace.

```python
def note_config_location(context):
    if context.config.config_dir is not None:
        context.metadata["config_dir"] = context.config.config_dir.name
    return context.current
```

### `names()`

`names()` returns available non-hidden TOML stems as a tuple, such as
`("runtime", "sources")`. If there is no workspace config directory, it returns
an empty tuple.

```python
def capture_config_names(context):
    context.metadata["config_names"] = context.config.names()
    return context.current
```

### `get(name)`

`get(name)` returns a parsed dictionary or `None` when the file is missing.
Names must be non-empty.

```python
def apply_optional_limit(context):
    cfg = context.config.get("runtime") or {}
    limit = cfg.get("limits", {}).get("max_rows")
    if limit is None:
        return context.current
    return context.current.head(limit)
```

### `require(name)`

`require(name)` returns the parsed dictionary or raises `FlowValidationError`
when the file is missing or config lookup is unavailable. Use it for config
that is part of the flow's contract.

```python
def load_required_destination(context):
    cfg = context.config.require("destination")
    context.metadata["target_table"] = cfg["table"]["name"]
    return context.current
```

Invalid TOML also raises `FlowValidationError`.

### `all()`

`all()` parses every available config file and returns a dictionary keyed by
file stem.

```python
def count_config_files(context):
    context.metadata["config_file_count"] = len(context.config.all())
    return context.current
```

Use `context.config` for environment-specific settings such as file names,
folder names, thresholds, batch sizes, SQL parameters, and external table
names. Keep the flow's orchestration shape in the `Flow(...)` chain.

## `database(...)`

`context.database(name)` returns an absolute, write-ready path beneath the
authored workspace's `databases/` directory. Parent directories are created
automatically.

```python
def database_path(context):
    return context.database("analytics/runtime.duckdb")
```

Rules:

- `name` must be relative
- `name` must be non-empty
- the helper is only available for authored workspace flows
- the helper returns a `Path`; your code owns the database connection lifecycle

```python
import duckdb


def write_summary(context):
    db_path = context.database("analytics/summary.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.register("input_frame", context.current)
        conn.sql("create or replace table summary as select count(*) as rows from input_frame")
    finally:
        conn.close()
    return db_path
```

## `source_metadata()`

`context.source_metadata()` returns filesystem metadata for the active source
file, or `None` when there is no concrete source file.

Returned fields:

- `path`
- `name`
- `size_bytes`
- `modified_at_utc`

```python
def capture_source_metadata(context):
    metadata = context.source_metadata()
    if metadata is not None:
        context.metadata["source_name"] = metadata.name
        context.metadata["source_size_bytes"] = metadata.size_bytes
        context.metadata["source_modified_at_utc"] = metadata.modified_at_utc.isoformat()
    return context.current
```

## `source`

`context.source` is a `SourceContext` for the active source namespace. It is
usually present for source-backed poll or scheduled flows, and it may be `None`
for manual or in-memory flows.

The read-oriented helpers return resolved paths and do not create directories:

```python
context.source.root
context.source.path
context.source.relative_path
context.source.dir
context.source.folder
context.source.with_suffix(".json")
context.source.with_extension(".json")
context.source.file("notes.json")
context.source.namespaced_file("details.json")
context.source.root_file("lookup.csv")
```

### Source properties

`root` is the watched source root. `path` is the concrete active source file
when the run has one. `relative_path` is the active source's path relative to
the source root.

`dir` is the namespace directory for files derived from the active source. If
the active source is `incoming/orders.csv`, `dir` points at
`incoming/orders/`.

`folder` is the active source file's parent folder, or the source root when no
relative path is available.

```python
def capture_source_shape(context):
    if context.source is None:
        return context.current
    context.metadata["source_root_name"] = context.source.root.name
    context.metadata["source_folder_name"] = context.source.folder.name
    return context.current
```

### Source file helpers

`with_suffix(...)` and `with_extension(...)` return the active source-relative
path with a new suffix. They require a concrete source file.

```python
def read_json_sidecar(context):
    sidecar = context.source.with_extension(".json")
    if sidecar.exists():
        return sidecar.read_text(encoding="utf-8")
    return None
```

`file(name)` returns a path in the active source file's parent folder.

```python
def read_neighbor_notes(context):
    notes = context.source.file("notes.json")
    return notes.read_text(encoding="utf-8") if notes.exists() else "{}"
```

`namespaced_file(name)` returns a path inside the active source namespace and
requires a concrete source file.

```python
def read_derived_input(context):
    details = context.source.namespaced_file("details.json")
    return details.read_text(encoding="utf-8") if details.exists() else "{}"
```

`root_file(name)` returns a path directly under the source root.

```python
def read_lookup(context):
    lookup = context.source.root_file("lookup.csv")
    return lookup
```

All source helper names must be relative and non-empty.

## `mirror`

`context.mirror` is a `MirrorContext` for the mirrored output namespace. It is
available when the flow uses `mirror(root=...)`.

The write-oriented helpers return resolved paths and create parent directories:

```python
context.mirror.root
context.mirror.source_path
context.mirror.relative_path
context.mirror.dir
context.mirror.folder
context.mirror.with_suffix(".parquet")
context.mirror.with_extension(".parquet")
context.mirror.file("summary.json")
context.mirror.namespaced_file("open.parquet")
context.mirror.root_file("latest.parquet")
```

### Mirror properties

`root` is the configured mirror root. `source_path` and `relative_path` identify
the source file being mirrored when the run has one.

`dir` is the output namespace for files derived from the active source. If the
source-relative file is `incoming/orders.csv`, `dir` points at
`<mirror-root>/incoming/orders/`.

`folder` is the mirrored parent folder for the active source file, or the
mirror root when no relative path is available.

### Mirror file helpers

`with_suffix(...)` and `with_extension(...)` return the canonical mirrored
source path with a new suffix. They require a concrete source file.

```python
def write_parquet(context):
    output = context.mirror.with_extension(".parquet")
    context.current.write_parquet(output)
    return output
```

Returning the written `Path` allows runtime and UI surfaces to record the step
output for inspection.

`file(name)` returns a custom file path in the mirrored source folder.

```python
def write_summary(context):
    output = context.mirror.file("summary.json")
    output.write_text('{"status": "ok"}', encoding="utf-8")
    return output
```

`namespaced_file(name)` returns a path inside the mirrored source namespace.
Use it for multiple outputs derived from one source.

```python
def write_split_outputs(context):
    open_path = context.mirror.namespaced_file("open.parquet")
    closed_path = context.mirror.namespaced_file("closed.parquet")
    context.current["open"].write_parquet(open_path)
    context.current["closed"].write_parquet(closed_path)
    return open_path
```

`root_file(name)` returns a path directly beneath the mirror root. Use it for
stable flow-level artifacts.

```python
def write_latest_snapshot(context):
    output = context.mirror.root_file("latest.parquet")
    context.current.write_parquet(output)
    return output
```

All mirror helper names must be relative and non-empty.

## Missing context surfaces

Not every flow has every context surface:

- a manual flow may have no `source`
- an in-memory scheduled flow may have no `source`
- a flow with no `mirror(root=...)` has no `mirror`
- `debug` is only present when debug artifact capture is configured for the run
- `database(...)` and `config` file lookup require an authored workspace

Write defensive checks when the flow shape permits those cases:

```python
def maybe_write_output(context):
    if context.mirror is None:
        return context.current
    output = context.mirror.root_file("latest.parquet")
    context.current.write_parquet(output)
    return output
```

## `debug`

`context.debug` is a `FlowDebugContext | None`. It writes debug artifacts for a
concrete flow run so the desktop app can preview intermediate data without
changing the main flow result.

Available helpers:

```python
context.debug.set_step("Clean rows")
context.debug.save_frame(frame, name="clean_rows", info={"stage": "clean"})
context.debug.save_json(payload, name="quality_report")
```

The runtime manages the active step label during normal execution, so flow
steps usually call only `save_frame(...)` or `save_json(...)`.

### `save_frame(...)`

`save_frame(frame, name=None, info=None)` accepts a Polars `DataFrame` or
`LazyFrame`. Lazy frames are collected before saving. The helper writes a
Parquet artifact plus linked metadata for in-app previewing, and returns the
artifact `Path`.

```python
def debug_filtered_rows(context):
    frame = context.current.filter(context.current["status"] == "open")
    if context.debug is not None:
        context.debug.save_frame(
            frame,
            name="open_rows",
            info={"rows": len(frame)},
        )
    return frame
```

Use `save_frame(...)` for inspectable tabular debug data. Use `save_as=` and
`use=` when later steps in the same flow need the value as normal
orchestration state.

### `save_json(...)`

`save_json(value, name=None, info=None)` writes a JSON debug artifact and
returns the artifact `Path`. Values and `info` are converted to JSON-safe
representations.

```python
def debug_quality_report(context):
    report = {"columns": list(context.current.columns), "rows": len(context.current)}
    if context.debug is not None:
        context.debug.save_json(report, name="quality_report")
    return context.current
```

## `FileRef`

`FileRef` is a thin wrapper around one resolved filesystem path. It is commonly
used for batch-oriented file collection and mapped steps.

Fields and helpers:

```python
file_ref.path
file_ref.name
file_ref.stem
file_ref.suffix
file_ref.parent
file_ref.exists()
str(file_ref)
```

`FileRef` also implements `__fspath__`, so APIs that accept filesystem paths
can usually accept a `FileRef` directly.

```python
def read_text_file(file_ref):
    if not file_ref.exists():
        return ""
    return file_ref.path.read_text(encoding="utf-8")
```

## `Batch`

`Batch[T]` is a small iterable container used instead of exposing raw lists by
default. `Flow.collect(...)` and `collect_files(...)` return `Batch[FileRef]`.

Supported operations:

```python
len(batch)
batch[0]
for item in batch:
    ...
batch.names()
batch.paths()
```

`names()` returns each item name when every item exposes a string `name`.
`paths()` returns each item path when every item exposes a `Path`-valued `path`.
Both raise `FlowValidationError` when an item does not expose the expected
shape.

```python
def capture_batch_manifest(context):
    batch = context.current
    context.metadata["file_names"] = batch.names()
    context.metadata["file_count"] = len(batch)
    return batch
```

Mapped steps often receive the item directly instead of the full context:

```python
def read_file(file_ref):
    return file_ref.path.read_text(encoding="utf-8")
```

For cookbook examples that include spreadsheet inputs, see the
[recipes guide](recipes.md).

## Practical walkthrough

This flow uses the core context surfaces together without hiding ordinary
Python work:

```python
import duckdb
import polars as pl

from data_engine import Flow


def read_source_file(file_ref):
    return pl.read_csv(file_ref.path)


def combine_files(context):
    cfg = context.config.get("runtime") or {}
    batch_size = cfg.get("limits", {}).get("batch_size", 5000)
    context.metadata["batch_size"] = batch_size
    return pl.concat(context.current, how="vertical_relaxed")


def summarize(context):
    db_path = context.database("documents/analytics.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.register("input_frame", context.current)
        summary = conn.sql("select count(*) as row_count from input_frame").pl()
    finally:
        conn.close()

    output = context.mirror.root_file("summaries/latest.parquet")
    summary.write_parquet(output)
    context.metadata["summary_path"] = str(output)

    if context.debug is not None:
        context.debug.save_frame(summary, name="summary", info={"rows": summary.height})
        context.debug.save_json({"output": str(output)}, name="summary_manifest")

    return output


def build():
    return (
        Flow(group="Documents")
        .watch(mode="schedule", run_as="batch", interval="15m", source="data/incoming")
        .mirror(root="data/outgoing")
        .collect([".csv"], save_as="source_files")
        .map(read_source_file, use="source_files", save_as="frames")
        .step(combine_files, use="frames", save_as="combined")
        .step(summarize, use="combined")
    )
```

This shape keeps orchestration in the `Flow(...)` chain while step code uses
`FlowContext` for runtime state, workspace settings, debug artifacts, and
source-aware paths.

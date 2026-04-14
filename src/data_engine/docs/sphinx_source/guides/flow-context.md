# FlowContext

`FlowContext` is the runtime object passed to every step.

It is the main place where the runtime meets your step code.

If you are authoring flows day to day, this is the surface you will use most often.

## What `FlowContext` contains

Common fields and helpers you will read directly:

- `flow_name`
- `group`
- `source`
- `mirror`
- `config`
- `database(...)`
- `current`
- `objects`
- `metadata`
- `source_metadata()`

Example:

```python
def inspect_context(context):
    print(context.flow_name)
    print(context.group)
    print(context.current)
    if context.source is not None:
        print(context.source.path)
    return context.current
```

## The three most important ideas

When in doubt, remember these three ideas:

1. `current` is the moving value in the pipeline.
2. `objects` is the named stash of saved intermediates.
3. `source` and `mirror` are path namespaces, not open files or connections.

Everything else in `FlowContext` builds on those ideas.

## `flow_name` and `group`

These are the flow identity fields available at runtime.

- `flow_name` comes from the flow-module filename
- `group` comes from `Flow(group=...)`

They are useful when you want to:

- stamp metadata
- label outputs
- branch behavior lightly by flow identity
- emit operator-facing details into `context.metadata`

## `current`

`context.current` is the moving runtime slot.

- before the first manual or scheduled step, it is `None`
- after each step, it becomes that step's return value
- if `use=` is set, the runtime loads the named object into `current` before running the step

This is why most steps are so small:

```python
def clean_claims(context):
    return context.current.filter(...)
```

The runtime always hands the step the current value.

## `objects`

Saved objects live in `context.objects`.

That is what `save_as=` and `use=` operate on.

Example:

```python
(
    Flow(group="Claims")
    .step(read_claims, save_as="raw_df")
    .step(clean_claims, use="raw_df", save_as="clean_df")
    .step(write_output, use="clean_df")
)
```

Inside a step you can also read those values directly:

```python
def compare_versions(context):
    raw_df = context.objects["raw_df"]
    clean_df = context.objects["clean_df"]
    ...
```

This is especially useful when a later step needs more than one previously saved object.

## `metadata`

`context.metadata` is a free-form runtime metadata dictionary.

Use it when a step wants to publish details about what happened during execution.

The runtime also seeds a few values automatically:

- `started_at_utc`
- `run_id`
- `step_outputs`
- `file_hash` when the run is bound to a concrete source file

`file_hash` is a stable SHA-1 hash of the source-relative path when one exists. For single-file bindings, it falls back to the concrete source path text.

Examples:

- row counts
- source metadata
- selected config values
- warning flags
- lightweight operator diagnostics

Example:

```python
def capture_stats(context):
    context.metadata["row_count"] = len(context.current)
    context.metadata["flow_name"] = context.flow_name
    return context.current
```

The runtime also records step output paths here when a step returns an existing `Path`.

That is what powers the UI `Inspect` button for a step: if a step writes a file and returns its existing path, the UI can enable inspection for that step.

## `config`

`context.config` is lazy read-only access to `config/*.toml` files in the current authored workspace.

Available helpers are:

```python
context.config.get("claims")
context.config.require("claims")
context.config.names()
context.config.all()
```

### `get(name)`

Returns a parsed `dict` or `None`.

Use this when the config file is optional:

```python
def apply_runtime_config(context):
    cfg = context.config.get("claims")
    if cfg is None:
        return context.current
    batch_size = cfg.get("runtime", {}).get("batch_size", 5000)
    context.metadata["batch_size"] = batch_size
    return context.current
```

### `require(name)`

Returns the parsed `dict` or raises when the file is missing.

Use this when the config is part of the flow's contract:

```python
def load_required_settings(context):
    cfg = context.config.require("database")
    dsn = cfg["connection"]["dsn"]
    context.metadata["dsn"] = dsn
    return context.current
```

### `names()`

Returns available config stems such as:

```python
("claims", "runtime")
```

This is mostly useful for introspection or diagnostics.

### `all()`

Returns every parsed config mapping keyed by file stem.

Example:

```python
all_config = context.config.all()
```

### What `config` is good for

`context.config` is a good fit for:

- file names and folder names
- thresholds and batch sizes
- optional feature flags
- SQL parameters
- external table names

`context.config` complements the `Flow(...)` chain. The orchestration shape still belongs in the fluent flow definition.

## `database(...)`

`context.database(...)` returns a write-ready path beneath `databases/` in the current authored workspace.

Example:

```python
db_path = context.database("claims/db.duckdb")
```

That resolves to:

- `workspaces/<workspace_id>/databases/claims/db.duckdb`

Rules:

- the path must be relative
- parent directories are created automatically
- the helper is only available for authored workspace flows
- it returns a `Path` for your step to open

Typical usage:

```python
import duckdb


def write_summary(context):
    db_path = context.database("claims/analytics.duckdb")
    conn = duckdb.connect(db_path)
    try:
        ...
    finally:
        conn.close()
```

This is intentionally simple. Data Engine gives you the path and your code owns the connection lifecycle.

## `source_metadata()`

`context.source_metadata()` returns basic filesystem metadata for the current source file when one exists.

It gives you:

- path
- file name
- size in bytes
- modified time in UTC

Example:

```python
def capture_source_info(context):
    metadata = context.source_metadata()
    if metadata is not None:
        context.metadata["source_name"] = metadata.name
        context.metadata["source_size_bytes"] = metadata.size_bytes
    return context.current
```

This is useful for audit trails, diagnostics, and output manifests.

## `source`

`context.source` is the input-side namespace for the active source.

It is usually present for poll flows and for scheduled flows that bind a source.

It may be `None` for manual flows or scheduled flows that build data entirely in memory.

Core helpers are:

```python
context.source.path
context.source.dir
context.source.folder
context.source.with_extension(".json")
context.source.with_suffix(".json")
context.source.file("notes.json")
context.source.namespaced_file("notes.json")
context.source.root_file("lookup.csv")
```

### `path`

The concrete active source file path.

This is the simplest and most direct read-side path:

```python
def read_claims(context):
    return pl.read_excel(context.source.path)
```

### `dir`

The namespace directory for files derived from the active source.

### `folder`

The active source file's parent folder.

### `with_extension(...)` and `with_suffix(...)`

These give you the same source-relative file with a new extension.

```python
def find_json_sidecar(context):
    return context.source.with_extension(".json")
```

### `file(...)`

Gives you a path in the active source file's parent folder.

```python
def find_notes(context):
    return context.source.file("notes.json")
```

### `namespaced_file(...)`

Gives you a path under the active source file's namespace.

```python
def find_namespaced_notes(context):
    return context.source.namespaced_file("notes.json")
```

### `root_file(...)`

Gives you a path directly under the source root.

```python
def load_lookup(context):
    return context.source.root_file("lookup.csv")
```

### Common `source` patterns

Use `source` when you need:

- the active input file
- a sidecar file near that input
- a lookup file under the watched source root
- namespace-aware paths derived from the current source item

## `mirror`

`context.mirror` is the mirrored output namespace for the active source.

It is present when the flow uses `mirror(root=...)`.

Core helpers are:

```python
context.mirror.root
context.mirror.dir
context.mirror.folder
context.mirror.with_extension(".parquet")
context.mirror.with_suffix(".parquet")
context.mirror.file("open_claims.parquet")
context.mirror.namespaced_file("open_claims.parquet")
context.mirror.root_file("analytics.duckdb")
```

### `with_extension(...)` and `with_suffix(...)`

These are for the common "mirror this source file into another format" case.

```python
def write_target(context):
    output = context.mirror.with_extension(".parquet")
    context.current.write_parquet(output)
    return output
```

Returning that written `Path` is what makes the step inspectable in the UI.

### `file(...)`

Use this for a custom file name in the mirrored source folder:

```python
def write_summary(context):
    summary_path = context.mirror.file("summary.json")
    summary_path.write_text("{}", encoding="utf-8")
    return summary_path
```

### `namespaced_file(...)`

Use this for multiple outputs derived from one source:

```python
def write_outputs(context):
    open_path = context.mirror.namespaced_file("open_claims.parquet")
    closed_path = context.mirror.namespaced_file("closed_claims.parquet")
    ...
```

### `root_file(...)`

Use this when you want one stable artifact under the mirror root for the whole flow.

```python
def write_snapshot(context):
    snapshot = context.mirror.root_file("artifacts/latest.parquet")
    context.current.write_parquet(snapshot)
    return snapshot
```

### Common `mirror` patterns

Use `mirror` when you want to:

- preserve source-relative output structure
- create many derived outputs from one source
- write stable summary artifacts under one output root
- avoid hand-building output folder math

All helpers return write-ready paths with parent directories prepared.

## When `source` or `mirror` may be missing

Not every flow has every context surface available.

Examples:

- a manual flow may have no `source`
- a purely in-memory scheduled flow may have no `source`
- a flow with no `mirror(root=...)` has no `mirror`

So it is reasonable to write defensive code when the flow shape allows those cases:

```python
def maybe_capture_source(context):
    if context.source is None:
        return context.current
    context.metadata["source_path"] = str(context.source.path)
    return context.current
```

## Batch values

`Flow.collect(...)` returns a `Batch` of `FileRef` items.

That means later steps can work with:

- `file_ref.name`
- `file_ref.path`
- `file_ref.stem`
- `file_ref.suffix`
- `file_ref.parent`

Example:

```python
def read_claims(file_ref):
    return pl.read_excel(file_ref.path)
```

When you are in a mapped step, the item is often simpler than the full `context`, and that is by design.

## A practical context walkthrough

Here is a representative flow using several parts of the context together:

```python
import duckdb
import polars as pl

from data_engine import Flow


def read_claims(file_ref):
    return pl.read_excel(file_ref.path)


def combine_claims(context):
    cfg = context.config.get("claims") or {}
    batch_size = cfg.get("runtime", {}).get("batch_size", 5000)
    context.metadata["batch_size"] = batch_size
    return pl.concat(context.current, how="vertical_relaxed")


def summarize(context):
    db_path = context.database("claims/analytics.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.register("input", context.current)
        summary = conn.sql("select count(*) as row_count from input").pl()
    finally:
        conn.close()
    output = context.mirror.file("summary.parquet")
    summary.write_parquet(output)
    context.metadata["summary_path"] = str(output)
    return output


def build():
    return (
        Flow(group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/claims_flat")
        .mirror(root="../../example_data/Output/example_summary")
        .collect([".xlsx"], save_as="claim_files")
        .map(read_claims, use="claim_files", save_as="claim_frames")
        .step(combine_claims, use="claim_frames", save_as="raw_df")
        .step(summarize, use="raw_df")
    )
```

That one flow uses:

- `Batch` and `FileRef`
- `current`
- `objects`
- `config`
- `database(...)`
- `mirror`
- `metadata`

That is the intended shape of the authoring model: small runtime helpers that make native Python data work easier to organize.

# Flow Methods

This page covers the small author-facing `Flow` surface.

The method-level reference now lives in the `Flow` docstrings and is rendered in
the API reference. Keep examples that describe exact parameters, return values,
and validation rules beside the methods in `src/data_engine/core/flow.py`; that
keeps VS Code hover help and the packaged docs in sync. This page is the
author-facing tour of when to use those methods together.

```python
from data_engine import Flow
```

## `Flow(group)`

Create a new immutable flow definition.

```python
flow = Flow(group="Claims")
```

Rules:

- `group` must be a non-empty string
- the flow-module filename provides the flow identity
- the returned object is immutable, so each fluent call returns a new `Flow`

Immutability matters because it keeps authoring predictable. Each chained call produces a new flow definition rather than mutating hidden shared state.

## `watch(...)`

Configure a runtime trigger for manual, poll, or schedule execution.

```python
flow = flow.watch(
    mode="poll",
    source="../../example_data/Input/claims_flat",
    interval="5s",
    extensions=[".xlsx", ".xlsm"],
    settle=1,
)
```

```python
flow = flow.watch(
    mode="poll",
    source="../../example_data/Settings/single_watch.xlsx",
    interval="5s",
)
```

```python
flow = flow.watch(mode="schedule", run_as="batch", interval="15m")
flow = flow.watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/claims_flat")
flow = flow.watch(mode="schedule", run_as="batch", time="10:31", source="../../example_data/Settings/single_watch.xlsx")
flow = flow.watch(mode="schedule", run_as="batch", time=["08:15", "14:45"])
flow = flow.watch(mode="manual")
```

Rules:

- `mode` must be one of `manual`, `poll`, or `schedule`
- `run_as` defaults to `individual`
- `run_as="individual"` means one run per concrete source file
- `run_as="batch"` means one run at the watched root
- poll flows require `source=` and `interval=`
- schedule flows accept exactly one of `interval=` or `time=`
- `time` accepts either one `HH:MM` string or a collection of `HH:MM` strings
- `extensions` and `settle` are poll-only options
- missing or bad paths fail now and recover later when the path becomes valid
- poll freshness compares the current source file signature against the runtime ledger

Practical guidance:

- use `manual` for explicit button-driven flows
- use `poll` when the source changing should be the trigger
- use `schedule` when time should be the trigger
- use `run_as="batch"` when the flow should reason about a folder or root as one unit
- use `run_as="individual"` when each source file should become its own run

`watch(...)` is where you describe orchestration intent, not transformation logic.

## `mirror(root=...)`

Bind a mirrored output namespace rooted at one directory.

```python
flow = flow.mirror(root="../../example_data/Output/example_mirror")
```

`mirror(...)` does not write files. It defines the output namespace exposed later through `context.mirror`.

You can omit `mirror(...)` entirely if the flow has no need for a mirrored output namespace.

## `step(fn, use=None, save_as=None, label=None)`

Add one generic callable step.

```python
flow = flow.step(read_claims, save_as="raw_df")
flow = flow.step(clean_claims, use="raw_df", save_as="clean_df")
flow = flow.step(write_output, use="clean_df", label="Write Parquet")
```

Rules:

- `fn` must be callable
- `fn` must accept exactly one `context` parameter
- `use=` selects a previously saved object
- `save_as=` stores the returned object
- `label=` overrides the UI display name

The return value always becomes `context.current`.

This is the default workhorse method. Most flows are easiest to read when they are mostly made of `step(...)` with occasional `collect(...)` and `map(...)` where batching is truly needed.

## `map(fn, use=None, save_as=None, label=None)`

Map one callable across the current batch.

```python
flow = flow.collect(extensions=[".pdf"])
flow = flow.map(fn=validate_pdf)
flow = flow.map(fn=validate_pdf_with_context, label="Validate Pdf")
```

```python
def validate_pdf(file_ref):
    return {"name": file_ref.name, "ok": file_ref.exists()}


def validate_pdf_with_context(context, file_ref):
    return {"flow": context.flow_name, "name": file_ref.name}
```

Rules:

- `map()` expects the current value to be iterable
- `fn` may accept either `(item)` or `(context, item)`
- the mapped results are returned as a `Batch`
- `map()` raises when the current batch is empty
- `use=`, `save_as=`, and `label=` work the same way they do for `step()`

Reach for `map(...)` when the same callable should run once per collected item. If the callable should reason about the whole collection, switch back to a normal `step(...)`.

## `step_each(fn, use=None, save_as=None, label=None)`

`step_each(...)` is an alias for `map(...)`.

Use whichever reads better in the flow module:

```python
flow = flow.map(fn=read_claims)
flow = flow.step_each(fn=read_claims)
```

## `collect(extensions, root=None, recursive=False, use=None, save_as=None, label=None)`

Collect matching files into a `Batch` of `FileRef` items.

```python
flow = flow.collect(extensions=[".xlsx"])
flow = flow.collect(extensions=[".pdf"], recursive=True)
```

Behavior:

- uses `root=` when provided
- otherwise falls back to `context.source.root`
- returns a `Batch`, not a raw list
- each item exposes `.name`, `.path`, `.stem`, `.suffix`, and `.parent`

If `root=` is omitted, the runtime falls back to the current source root. That is often the cleanest choice for poll or scheduled batch flows already bound to a source.

## `run_once()`

Run the flow one time and return the completed contexts.

Use this when you want a one-off Python-driven execution rather than continuous watching.

## `run()`

Start continuous execution for watched poll or schedule flows.

This is the entrypoint behind long-lived runtime behavior.

## `preview(use=None)`

Run one flow for notebook inspection and return a real object.

```python
build().preview()
build().preview(use="raw_df").head(10)
build().preview(use="claim_frames")
```

Behavior:

- without `use=`, returns the final `context.current`
- with `use="name"`, runs only until `save_as="name"` exists
- returns the real saved object, so dataframe methods like `.head(10)` work naturally
- avoids running later write/debug steps once the requested saved object is available
- if a poll flow would have several startup source files, preview uses the first deterministic source candidate for notebook inspection rather than trying to preview every file at once

`preview(...)` is especially useful while authoring notebook-backed flows because it lets you stop at a meaningful intermediate instead of running the whole flow to the final writer step every time.

## `show()`

Preview the single current result from a one-off flow.

Use this for quick interactive inspection when the final current value itself is the thing you want to see.

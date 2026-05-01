# Flow Methods

This page is the author-facing tour of the `Flow` builder surface. The method
docstrings remain the source of truth for exact signatures and API reference
rendering; use this guide for examples and rules that matter while writing flow
modules.

```python
from data_engine import Flow
```

## `Flow(group, name=None, label=None)`

Create a new immutable flow definition.

```python
flow = Flow(group="Reports")
flow = Flow(group="Reports", label="Daily Report Build")
```

Rules:

- `group` must be a non-empty string.
- `name`, when provided, must be a non-empty string.
- `label`, when provided, must be a non-empty string and is the human-readable
  flow title shown by operator surfaces.
- When a flow module omits `name`, the module loader can derive the stable flow
  identity from the module filename.
- Each builder method returns a new `Flow`; the previous instance is unchanged.

Use `label=` when the UI title should be friendlier than the module identity.
Keep `name=` stable when code outside the module needs to refer to the flow by
identifier.

## `watch(...)`

Configure how the runtime starts flow runs.

```python
manual_flow = Flow(group="Reports").watch(mode="manual")

poll_flow = Flow(group="Reports").watch(
    mode="poll",
    source="incoming",
    interval="30s",
    extensions=[".csv", "json"],
    settle=2,
    max_parallel=4,
)

scheduled_flow = Flow(group="Reports").watch(
    mode="schedule",
    run_as="batch",
    source="incoming",
    time=["08:00", "16:30"],
)
```

Parameters:

- `mode` is required and must be `"manual"`, `"poll"`, or `"schedule"`.
- `run_as` defaults to `"individual"`. Use `"individual"` for one run per
  concrete source file, or `"batch"` for one run against the watched source
  root.
- `max_parallel` controls how many source-file runs one individual-mode flow can
  execute concurrently. It must be an integer greater than zero.
- `source` is a file or directory path used by source-backed runs.
- `interval` accepts positive duration strings such as `"500ms"`, `"30s"`,
  `"5m"`, `"2h"`, `"1d"`, or `"1w"`.
- `time` accepts one `HH:MM` string or a collection of `HH:MM` strings.
- `extensions` limits source discovery to matching suffixes. Values are
  normalized to lowercase and may be written with or without the leading dot.
- `settle` is the poll-only delay, in seconds, before a changed file is queued.
  It must be an integer greater than or equal to zero.

Mode rules:

- `manual` rejects `interval=`, `time=`, and custom `settle=` values.
- `poll` requires both `source=` and `interval=`, and rejects `time=`.
- `schedule` accepts exactly one of `interval=` or `time=`.
- `schedule` may include `source=` when the scheduled run should be
  source-backed.
- `poll` and `schedule` both support `run_as="individual"` and
  `run_as="batch"`.

Use manual flows for button-driven or explicit Python execution, poll flows when
file changes should trigger work, and scheduled flows when time should trigger
work. Use batch mode when the step logic needs to reason about a whole folder or
root as one unit.

## `mirror(root=...)`

Bind a mirrored output namespace rooted at one directory.

```python
flow = Flow(group="Reports").mirror(root="processed")
```

`mirror(...)` configures `context.mirror`; it does not write files by itself.
Steps can use the mirror helpers to build write-ready paths for any output type:

```python
def write_summary(context):
    output_path = context.mirror.with_suffix(".parquet")
    context.current.write_parquet(output_path)
    return output_path
```

You can omit `mirror(...)` when a flow has no mirrored output namespace. `root`
may be a string or `Path`.

## `step(fn, use=None, save_as=None, label=None)`

Append one callable step.

```python
def read_source(context):
    return context.source.path.read_text(encoding="utf-8")


def parse_rows(context):
    return [line.split(",") for line in context.current.splitlines()]


def write_rows(context):
    output_path = context.mirror.root_file("rows.json")
    output_path.write_text(str(context.current), encoding="utf-8")
    return output_path


flow = (
    Flow(group="Reports")
    .watch(mode="poll", source="incoming", interval="30s", extensions=[".csv"])
    .mirror(root="processed")
    .step(read_source, save_as="raw_text", label="Read Source")
    .step(parse_rows, use="raw_text", save_as="rows", label="Parse Rows")
    .step(write_rows, use="rows", label="Write Output")
)
```

Rules:

- `fn` must be callable and must accept exactly one `context` parameter.
- The callable return value becomes the next `context.current`.
- `save_as=` stores the return value in `context.objects` under that name.
- `use=` loads a previously saved object into `context.current` before the step
  runs.
- `label=` overrides the display name used in logs, UI summaries, and debug
  artifact grouping.

`step(...)` is the default workhorse. It is the right choice when a callable
needs the full `FlowContext` or needs to operate on the current value as one
object.

## `collect(extensions, root=None, recursive=False, use=None, save_as=None, label=None)`

Append a step that collects matching files into a `Batch` of `FileRef` items.

```python
flow = (
    Flow(group="Reports")
    .watch(mode="schedule", run_as="batch", source="incoming", interval="15m")
    .collect([".csv", ".json"], recursive=True, save_as="source_files")
)
```

Behavior:

- `extensions` is required and must include at least one non-empty extension.
- `root=` chooses the search root.
- When `root=` is omitted, collection uses `context.source.root`.
- `recursive=True` searches child directories.
- Missing collection roots return an empty `Batch`.
- Each `FileRef` exposes `.path`, `.name`, `.stem`, `.suffix`, `.parent`, and
  `.exists()`.
- `Batch` is iterable and also exposes `.items`, `.names()`, and `.paths()`.

`collect(...)` accepts the same `use=`, `save_as=`, and `label=` step options as
`step(...)`.

## `map(fn, use=None, save_as=None, label=None)`

Append a step that maps a callable across the current iterable value.

```python
def summarize_file(file_ref):
    return {"name": file_ref.name, "bytes": file_ref.path.stat().st_size}


flow = (
    Flow(group="Reports")
    .collect([".csv"], root="incoming", save_as="files")
    .map(summarize_file, use="files", save_as="summaries", label="Summarize Files")
)
```

The mapped callable may accept either `item` or `context, item`:

```python
def tag_file(context, file_ref):
    return {"flow": context.flow_name, "name": file_ref.name}


flow = Flow(group="Reports").collect([".json"], root="incoming").map(tag_file)
```

Rules:

- `fn` must be callable and accept either one or two parameters.
- The current value must be iterable.
- Strings, bytes, dictionaries, and `None` are rejected as map inputs.
- Empty iterables are rejected.
- Results are returned as a `Batch`.
- `use=`, `save_as=`, and `label=` work the same way they do for `step(...)`.

Use `map(...)` when the same operation should run once per item. Use
`step(...)` when a callable should combine, reduce, or validate the whole
collection.

## `step_each(fn, use=None, save_as=None, label=None)`

`step_each(...)` is an alias for `map(...)`.

```python
flow = Flow(group="Reports").collect([".csv"], root="incoming").step_each(summarize_file)
```

Choose whichever name makes the chain read better. The signature, validation,
and returned `Batch` behavior are the same as `map(...)`.

## `run_once()`

Run the flow once and return completed `FlowContext` objects.

```python
contexts = flow.run_once()
first_result = contexts[0].current
```

This is useful for one-off Python-driven execution. A source-backed individual
flow may return more than one context, one for each executed source.

## `run()`

Run continuously according to the flow trigger.

```python
Flow(group="Reports").watch(mode="poll", source="incoming", interval="30s").run()
```

Use this for long-lived poll or schedule execution from Python. For running
several flows together, import the module-level `run` helper:

```python
from data_engine import Flow, run

run(flow_a, flow_b)
```

Grouped execution runs flows sequentially within each group and allows different
groups to run in parallel.

## `preview(use=None)`

Run one flow for authoring-time inspection and return a real object.

```python
build().preview()
build().preview(use="raw_text")
build().preview(use="summaries")
```

Behavior:

- Without `use=`, preview returns the final `context.current`.
- With `use="name"`, preview runs only until that `save_as` slot exists.
- Later write/debug steps are skipped once the requested object is available.
- If a poll flow has several startup source files, preview uses the first
  deterministic source candidate.
- `preview(...)` is not available from inside compiled flow modules.

`preview(...)` is for notebooks, REPLs, and direct flow-module authoring. Use
`run_once()` when you need completed runtime contexts rather than just the
previewed value.

## Shared Validation Caveats

The fluent methods validate early so flow modules fail clearly while loading.

- `use=` and `save_as=` must be non-empty strings when provided.
- `save_as="current"` is reserved and rejected.
- `use=` references are resolved at runtime; a step fails if the named object has
  not been saved by an earlier step in that run.
- Step labels must be non-empty strings when provided.
- Callable labels default to a title-cased version of the function or callable
  object name.
- Relative paths are resolved from the flow authoring context when available;
  prefer project-relative paths in examples and committed flow modules.
- `collect(...)` may produce an empty `Batch`, but `map(...)` rejects empty input.

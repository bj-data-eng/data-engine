# Configuring Flows

Per-flow configuration lives in the fluent `Flow` chain, not in TOML.

That is an important design choice:

- the runtime shape of a flow belongs in the authored `Flow(...)` definition
- workspace-local TOML in `config/` is for step logic and runtime parameters consumed by your code
- there is no separate "expand this flow into several configured variants" layer after `build()`

## Core fields

```python
Flow(group="Claims")
```

`group` is author-defined. The flow-module filename provides the flow identity.

Use `group` to cluster related flows in the UI and runtime model. A good rule of thumb is that a group should mean "these flows belong to the same operator-facing area of work."

## Watching

Single-file polling:

```python
Flow(group="Settings").watch(
    mode="poll",
    source="../../example_data/Settings/single_watch.xlsx",
    interval="5s",
).mirror(
    root="../../example_data/Output/example_single_watch",
)
```

Directory polling:

```python
Flow(group="Claims").watch(
    mode="poll",
    source="../../example_data/Input/claims_flat",
    interval="5s",
    extensions=[".xlsx", ".xls", ".xlsm"],
    settle=1,
).mirror(
    root="../../example_data/Output/example_mirror",
)
```

Scheduled batch runs:

```python
Flow(group="Analytics").watch(
    mode="schedule",
    run_as="batch",
    interval="15m",
    source="../../example_data/Input/claims_flat",
).mirror(
    root="../../example_data/Output/example_summary",
)

Flow(group="Settings").watch(
    mode="schedule",
    run_as="batch",
    time="10:31",
    source="../../example_data/Settings/single_watch.xlsx",
).mirror(
    root="../../example_data/Output/example_schedule",
)

Flow(group="Settings").watch(
    mode="schedule",
    run_as="batch",
    time=["08:15", "14:45"],
    source="../../example_data/Settings/single_watch.xlsx",
)
```

What watching owns:

- source selection
- ledger-backed source freshness tracking
- extension filtering for poll directory sources
- settle/debounce behavior for poll flows
- whether runtime executes per file or as one root-level batch via `run_as=`

What watching does not own:

- dataframe reads
- dataframe transforms
- database work
- output writing

That separation is what keeps `watch(...)` readable. It tells the engine when and why to run, not how to do the underlying data work.

`watch(mode="schedule", ...)` accepts exactly one of:

- `interval="10m"`
- `time="HH:MM"`
- `time=["08:15", "14:45"]`

It may also bind an optional `source=...` path for recurring jobs.

### `run_as`

`run_as` controls what the runtime treats as one unit of work.

Common values are:

- `run_as="individual"`: one run per concrete source file
- `run_as="batch"`: one run at the watched root

Use `individual` when each source file should be processed independently.

Use `batch` when the flow should reason about the watched source as one collection, such as "all current workbooks in this folder."

### Poll-specific options

`extensions=` limits which files in a polled directory participate in freshness checks and execution.

`settle=` adds debounce behavior so the engine does not immediately react to a file that is still being written by another process.

## Mirror bindings

Use `mirror(root=...)` when a flow needs source-relative output routing.

Inside steps:

```python
context.mirror.with_suffix(".parquet")
context.mirror.file("summary.json")
context.mirror.namespaced_file("open_claims.parquet")
context.mirror.root_file("analytics.duckdb")
```

`mirror(...)` does not write files. It only defines the output namespace available at runtime.

If a flow has no natural mirrored outputs, you do not need `mirror(...)`.

If a flow writes several related outputs, `mirror(...)` is usually the cleanest way to keep them organized without scattering path math through your steps.

## Batch workflows

Use `collect(...)` and `map(...)` or `step_each(...)` together for folder-oriented processing:

```python
Flow(group="Analytics") \
    .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/claims_flat") \
    .collect([".xlsx"], save_as="claim_files") \
    .map(read_claims, use="claim_files", save_as="claim_frames")
```

`map(...)` is the per-item stage in that pipeline, and `step_each(...)` is the equivalent alias. Both raise immediately when the batch is empty.

This is the standard batch shape:

1. watch a directory or scheduled source root
2. collect matching files into a `Batch`
3. map one callable across each file
4. switch back to `step(...)` once you want to reason about the combined result

## Configuring step labels and saved objects

Flow configuration also includes the names and labels you assign in the chain.

Examples:

```python
(
    Flow(group="Claims")
    .step(read_claims, save_as="raw_df")
    .step(clean_claims, use="raw_df", save_as="clean_df")
    .step(write_output, use="clean_df", label="Write Parquet")
)
```

Those fields affect the authoring experience directly:

- `save_as=` creates stable names for later steps and notebook previews
- `use=` loads one of those saved names into `context.current`
- `label=` controls the display name in the UI

If you are deciding where a piece of information belongs:

- if it shapes orchestration, put it in the `Flow` chain
- if it shapes step logic, put it in your code or in `context.config`

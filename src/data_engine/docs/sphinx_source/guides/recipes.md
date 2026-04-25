# Recipes

This page collects complete end-to-end examples.

When a recipe matches a shipped starter flow, the starter flow name is called out explicitly.

For quick dataframe inspection while authoring, `save_as=`/`use=` are the
notebook-friendly way to pause on a named intermediate, and
`context.debug.save_frame(...)` is the runtime-friendly way to keep a dataframe
visible in the app's Debug view.

## Recipe: Mirror every workbook

Starter flow: `example_mirror`

```python
from data_engine import Flow
import polars as pl


def read_docs(context):
    return pl.read_excel(context.source.path)


def write_target(context):
    output = context.mirror.with_suffix(".parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Docs")
        .watch(
            mode="poll",
            source="../../example_data/Input/docs_flat",
            interval="5s",
            extensions=[".xlsx", ".xlsm"],
        )
        .mirror(root="../../example_data/Output/example_mirror")
        .step(read_docs, label="Read Excel")
        .step(write_target, label="Write Parquet")
    )
```

Why this pattern is useful:

- poll reacts to new or changed source files
- `mirror.with_suffix(...)` preserves source-relative output naming
- returning the parquet path makes the output inspectable in the UI

## Recipe: Filter rows and write a cleaned output

Starter flow: `example_completed`

```python
import polars as pl


def read_docs(context):
    return pl.read_excel(context.source.path)


def keep_completed(context):
    return context.current.filter(pl.col("Step TO") == "COMPLETED")


def write_target(context):
    output = context.mirror.with_suffix(".parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Docs")
        .watch(
            mode="poll",
            source="../../example_data/Input/docs_flat",
            interval="5s",
            extensions=[".xlsx", ".xlsm"],
        )
        .mirror(root="../../example_data/Output/example_completed")
        .step(read_docs, save_as="raw_df")
        .step(keep_completed, use="raw_df", save_as="clean_df")
        .step(write_target, use="clean_df")
    )
```

This is the classic "read -> filter -> write" shape, and it is a good default when you want clear previewable intermediates.

If you also want the filtered dataframe to appear in the desktop app's Debug
view during real runs, you can save it there too:

```python
def keep_completed(context):
    frame = context.current.filter(pl.col("Step TO") == "COMPLETED")
    if context.debug is not None:
        context.debug.save_frame(frame, name="clean_df", info={"rows": frame.height})
    return frame
```

## Recipe: Capture source metadata during processing

Starter flow: `example_metadata`

```python
def read_docs(context):
    return pl.read_excel(context.source.path)


def capture_source_info(context):
    metadata = context.source_metadata()
    if metadata is not None:
        context.metadata["source_name"] = metadata.name
        context.metadata["source_size_bytes"] = metadata.size_bytes
    return context.current
```

This is useful when you want provenance details recorded in `context.metadata` without changing the main pipeline object.

## Recipe: Produce a stable latest snapshot

Starter flow: `example_snapshot`

```python
def write_latest_snapshot(context):
    snapshot = context.mirror.root_file("artifacts/example_snapshot.parquet")
    context.current.write_parquet(snapshot)
    return snapshot
```

Use `mirror.root_file(...)` when the result should be one stable artifact for the whole flow.

## Recipe: Read selected worksheets from a multi-sheet workbook

Starter flow: `example_multisheet`

```python
def read_selected_sheets(context):
    return pl.read_excel(context.source.path, sheet_name=["Docs", "Summary"])
```

This is a good reminder that step code stays native and can call the underlying dataframe library directly.

## Recipe: Single-file settings workflow

Starter flows: `example_single_watch` and `example_schedule`

```python
def read_settings(context):
    return pl.read_excel(context.source.path)


def write_settings(context):
    output = context.mirror.with_suffix(".parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Settings")
        .watch(
            mode="schedule",
            run_as="batch",
            interval="15m",
            source="../../example_data/Settings/single_watch.xlsx",
        )
        .mirror(root="../../example_data/Output/example_schedule")
        .step(read_settings, save_as="settings_df")
        .step(write_settings, use="settings_df", label="Write Parquet")
    )
```

This is the right shape when the flow should rerun on a schedule against one well-known source file.

## Recipe: Batch read with `map(...)` or `step_each(...)`

Starter flow shape: `example_summary`

```python
from data_engine import Flow
import polars as pl


def read_docs(file_ref):
    return pl.read_excel(file_ref.path)


def combine_docs(context):
    return pl.concat(context.current, how="vertical_relaxed")


def build():
    return (
        Flow(group="Analytics")
        .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/docs_flat")
        .collect([".xlsx"], save_as="doc_files")
        .map(read_docs, use="doc_files", save_as="doc_frames")
        .step(combine_docs, use="doc_frames")
    )
```

`map(...)` is the right tool when the same callable should run once per collected file, and `step_each(...)` is the equivalent alias. Both raise immediately when the batch is empty.

## Recipe: Load into DuckDB and export a summary

Starter flow: `example_summary`

```python
import duckdb


def read_docs(file_ref):
    return pl.read_excel(file_ref.path)


def combine_docs(context):
    return pl.concat(context.current, how="vertical_relaxed")


def build_summary(context):
    conn = duckdb.connect(context.database("analytics.duckdb"))
    try:
        conn.register("input", context.current)
        return conn.sql(
            """
            select
                workflow,
                count(*) as row_count
            from input
            group by workflow
            order by row_count desc
            """
        ).pl()
    finally:
        conn.close()


def write_summary(context):
    output = context.mirror.file("workflow_summary.parquet")
    context.current.write_parquet(output)
    return output


def build():
    return (
        Flow(group="Analytics")
        .watch(mode="schedule", run_as="batch", interval="15m", source="../../example_data/Input/docs_flat")
        .mirror(root="../../example_data/Output/example_summary")
        .collect([".xlsx"], save_as="doc_files")
        .map(read_docs, use="doc_files", save_as="doc_frames")
        .step(combine_docs, use="doc_frames", save_as="raw_df")
        .step(build_summary, use="raw_df", save_as="summary_df")
        .step(write_summary, use="summary_df")
    )
```

That last example is a good place to prefer `context.database(...)`, because the DuckDB file is acting like a workspace-local database asset with a stable workspace home.

## Recipe: Use TOML workspace config

```python
def apply_threshold(context):
    cfg = context.config.require("docs")
    threshold = cfg.get("filters", {}).get("minimum_amount", 0)
    return context.current.filter(pl.col("amount") >= threshold)
```

This is a clean way to keep operator-tunable values out of the flow chain while still making the dependency explicit.

## Recipe: Save an intermediate dataframe to the Debug view

```python
import polars as pl


def calculate_totals(context):
    frame = context.current.with_columns(
        total=pl.col("amount") + pl.col("tax")
    )
    if context.debug is not None:
        context.debug.save_frame(frame, name="totals_df", info={"rows": frame.height})
    return frame
```

Use this pattern when the dataframe is worth inspecting in the app but you do
not want to turn the debug artifact into the flow's primary output.

## Recipe: Calculate business days and keep a grouped running total

```python
from datetime import date

import data_engine.helpers
import polars as pl


df = (
    df.sort(["claim_id", "sequence_number"])
    .with_columns(
        cumulative_business_days=
        pl.when(pl.col("use_days"))
        .then(
            data_engine.helpers.networkdays(
                "start_date",
                "end_date",
                holidays=[date(2026, 4, 15)],
            )
        )
        .otherwise(pl.lit(0))
        .cum_sum()
        .over("claim_id")
    )
)
```

This keeps the per-row business-day increment conditional, while the running
total continues to accumulate within each group.

## Recipe: Offset to the next business due date

```python
from datetime import date

import data_engine.helpers


df = df.with_columns(
    due_date=data_engine.helpers.workday(
        "received_date",
        "sla_days",
        holidays=[date(2026, 4, 15)],
        count_first_day=True,
    )
)
```

Use `count_first_day=True` when the received day itself should count as day 1
for SLA-style deadlines.

## Recipe: Propagate the last matching row value across a window

```python
import data_engine.helpers
import polars as pl


df = df.with_columns(
    archived_at=data_engine.helpers.propagate_last_value(
        pl.col("archive_date").dt.combine(pl.col("archive_time")),
        by="claim_id",
        sort_by="claim_step_index",
        where=pl.col("status") == "Archive",
    )
)
```

Use this when one row in a grouped window marks an event, but every row in
that window needs the event's value. The first argument can be a column name or
any Polars expression, including an adjacent value expression such as
`pl.col("archive_date").dt.combine(pl.col("archive_time"))`. The output column
name comes from `with_columns`.

## Recipe: Count repeated visits to the same workflow

```python
import data_engine.helpers


df = df.with_columns(
    workflow_visit=data_engine.helpers.visit_counter(
        "workflow",
        by="document_id",
        sort_by="step_index",
    )
)
```

Use this when a document can leave and later return to the same workflow. For
ordered workflow values `w1, w1, w1, w2, w2, w1, w1, w1`, the visit counter is
`1, 1, 1, 1, 1, 2, 2, 2`: the second `w1` run is visit 2 for `w1`, while the
first `w2` run remains visit 1 for `w2`.

## Recipe: Write several outputs for one source

```python
def write_outputs(context):
    open_path = context.mirror.namespaced_file("open_docs.parquet")
    closed_path = context.mirror.namespaced_file("closed_docs.parquet")
    context.current.filter(pl.col("status") == "OPEN").write_parquet(open_path)
    context.current.filter(pl.col("status") == "CLOSED").write_parquet(closed_path)
    return open_path
```

Use `namespaced_file(...)` when one source item naturally produces several derived outputs.

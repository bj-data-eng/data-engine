# Polars And Schema Helpers

`data_engine.helpers.polars` and `data_engine.helpers.schema` provide compact
authoring helpers for common Polars cleanup, date, window, and write patterns.

The function-by-function reference lives in the helper docstrings and is
rendered in the API reference. Keep exact signatures, edge cases, and copyable
examples beside the functions in `src/data_engine/helpers/polars.py` and
`src/data_engine/helpers/schema.py`; this guide explains the shared shape and
when each helper family fits.

Most helpers are available in two styles:

```python
import data_engine.helpers

df = df.with_columns(
    due_date=data_engine.helpers.workday("received_date", "sla_days")
)
```

```python
import data_engine.helpers

df = df.with_columns(
    due_date=df.de.workday("received_date", "sla_days")
)
```

Importing `data_engine.helpers` registers the Polars `.de` namespaces for
`pl.DataFrame` and `pl.LazyFrame`.

## Column Cleanup

Use `remove_null_columns(...)` after ingesting sparse or evolving inputs where
some columns are present but entirely empty.

```python
clean = data_engine.helpers.remove_null_columns(df)
clean = df.de.remove_null_columns()
```

Behavior:

- keeps columns that contain at least one non-null value
- removes all columns from a zero-row eager dataframe
- returns a lazy frame when given a lazy frame
- collects a lightweight non-null count for lazy inputs to decide which columns
  to keep

Use `normalize_column_names(...)` when source files vary in capitalization,
spacing, or separator placement.

```python
df = data_engine.helpers.normalize_column_names(df)
df = df.de.normalize_column_names()
```

Normalization trims the source name, removes spaces around common separators,
collapses remaining whitespace, replaces spaces with underscores, and lowercases
the result.

```python
assert data_engine.helpers.normalize_column_name(" Workflow / To ") == "workflow/to"
```

Pass `columns=[...]` to normalize only a selected subset of existing columns.
Missing selected columns are ignored.

## Business-Day Expressions

`networkdays(...)` and `workday(...)` return Polars expressions. They can be
used in `with_columns`, `select`, filters, and window expressions.

Use `networkdays(...)` when you need an Excel-style count of business days
between two dates.

```python
from datetime import date

df = df.with_columns(
    business_days=data_engine.helpers.networkdays(
        "received_date",
        "closed_date",
        holidays=[date(2026, 1, 1)],
    )
)
```

Use `workday(...)` when you need the business date a signed number of working
days before or after a start date.

```python
df = df.with_columns(
    due_date=df.de.workday(
        "received_date",
        "sla_days",
        holidays=["2026-01-01"],
    )
)
```

Shared behavior:

- date and datetime scalars, column names, and Polars expressions are accepted
- datetime values are normalized to their calendar date
- holidays can be `date`, `datetime`, or ISO date strings
- the default business-day mask counts Monday through Friday
- `mask=(True, True, True, True, True, True, False)` also counts Saturday
- `count_first_day=True` allows the start date to count as day 1

The helpers validate that custom masks contain exactly seven real boolean
values in Monday-first order.

## Ordered Window Helpers

Use `propagate_last_value(...)` and `propagate_first_value(...)` when one row in
an ordered group carries a value that should be visible on every row in the same
group.

```python
df = df.with_columns(
    latest_status=data_engine.helpers.propagate_last_value(
        "status",
        by="claim_id",
        sort_by="step_index",
    )
)
```

```python
df = df.with_columns(
    first_reviewer=df.de.propagate_first_value(
        "reviewer",
        by=["claim_id", "workflow"],
        sort_by=["step_index", "event_time"],
        where=pl.col("reviewer").is_not_null(),
    )
)
```

Behavior:

- rows are ordered inside each `by` window by `sort_by`
- `where=...` limits which ordered rows can supply the propagated value
- null values are skipped by default with `ignore_nulls=True`
- `descending` and `nulls_last` are forwarded to Polars ordering
- the result is a normal expression suitable for `with_columns` or `select`

Use `visit_counter(...)` when consecutive runs of the same value should share a
visit number, and later returns to that value should increment it.

```python
df = df.with_columns(
    workflow_visit=data_engine.helpers.visit_counter(
        "workflow",
        by="document_id",
        sort_by="step_index",
    )
)
```

For ordered values `w1, w1, w1, w2, w2, w1`, the result is `1, 1, 1, 1, 1, 2`.
The count is per current value inside the `by` window, so the first contiguous
run of each distinct value is visit 1.

## Atomic Writes And Sinks

Use atomic write helpers when downstream readers should never observe a partial
parquet or Excel file. Each helper writes to a unique temporary file beside the
target, then replaces the target with `os.replace`.

```python
target = data_engine.helpers.write_parquet_atomic(
    df,
    context.mirror.root_file("curated/docs.parquet"),
)
```

```python
target = df.de.write_excel_atomic(
    context.mirror.root_file("curated/docs.xlsx"),
    worksheet="Docs",
    table_name="docs",
    autofit=True,
)
```

```python
target = lf.de.sink_parquet_atomic(
    context.mirror.root_file("curated/docs.parquet"),
)
```

Available helpers:

- `write_parquet_atomic(df, path, **write_options)`
- `write_excel_atomic(df, path, worksheet=None, **write_options)`
- `sink_parquet_atomic(lf, path, **sink_options)`
- `df.de.write_parquet_atomic(path, **write_options)`
- `df.de.write_excel_atomic(path, worksheet=None, **write_options)`
- `lf.de.sink_parquet_atomic(path, **sink_options)`

Keyword options are forwarded to the matching Polars writer. Lazy parquet sinks
must execute eagerly; `sink_parquet_atomic(..., lazy=True)` raises `ValueError`
because replacement must complete inside the helper call.

## Namespace Wrappers

The `.de` namespace keeps dataframe chains compact while still calling the same
public helper functions.

Dataframe and lazy-frame namespaces both include:

- `normalize_column_names(...)`
- `remove_null_columns()`
- `networkdays(...)`
- `workday(...)`
- `propagate_last_value(...)`
- `propagate_first_value(...)`
- `visit_counter(...)`

`pl.DataFrame.de` also wraps eager writers, Excel composition, and DuckDB helper
operations that can use the dataframe directly. `pl.LazyFrame.de` wraps
`sink_parquet_atomic(...)`, Excel composition, and DuckDB operations that collect
or persist the lazy frame as part of the helper call.

For DuckDB-specific behavior, see [DuckDB Helpers](duckdb-helpers.md).

## TableSchema

`TableSchema` is a small column cleanup helper. It stores an explicit projection,
dtype casts, rename mapping, and drop list. Each part has its own `apply(...)`
method so flow code controls the cleanup order.

```python
import polars as pl

from data_engine.helpers import TableSchema

schema = TableSchema(
    columns=("claim_id", "status", "event_time"),
    dtypes={"claim_id": pl.Int64, "event_time": pl.Datetime},
    rename={"Claim Id": "claim_id", "Status": "status"},
    drop=("ssn", "notes"),
)

df = schema.drop.apply(df)
df = schema.rename.apply(df)
df = schema.dtypes.apply(df)
df = schema.columns.apply(df)
```

Parts:

- `schema.columns` is a `ColumnSelection` tuple with `apply(df)` and
  `normalize_column_names(df)`
- `schema.dtypes` is a `ColumnCasts` mapping whose `apply(df)` casts configured
  columns and casts remaining columns to `pl.String`
- `schema.rename` is a `RenameColumns` mapping whose `apply(df)` renames
  configured columns
- `schema.drop` is a `DropColumns` tuple whose `apply(df)` drops only columns
  that are present

Constructor inputs are normalized:

- column, dtype, rename, and drop names are converted to stripped strings
- empty names raise `ValueError`
- duplicate names in one schema part raise `ValueError`
- `columns` preserves the explicit projection order

Normalize source names first when incoming files have inconsistent headers:

```python
df = schema.normalize_column_names(df)
df = schema.dtypes.apply(df)
```

`TableSchema` works with eager and lazy Polars frames. It intentionally leaves
ordering decisions to the caller because ingest flows often need to cast, drop,
rename, persist, and project at different points in a chain.

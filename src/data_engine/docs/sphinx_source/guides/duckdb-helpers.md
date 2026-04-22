# DuckDB Helpers

`data_engine.helpers.duckdb` is the first public helper layer for common warehouse-style authoring patterns.

The function-by-function reference lives in the helper docstrings and is
rendered in the API reference. Keep signature details and copyable examples
beside the functions in `src/data_engine/helpers/duckdb.py`; that keeps editor
hover help and the packaged docs aligned. This page explains the shared design
shape and when the helper family is a good fit.

These helpers are intentionally:

- one-shot
- explicit about the database path
- explicit about the target table
- responsible for their own connection lifecycle

That means each helper:

- opens DuckDB
- does one job
- commits or rolls back
- closes the connection

They are designed for flow code that wants less repeated SQL plumbing without hiding too much behavior.

## Import style

```python
from data_engine.helpers.duckdb import build_dimension
from data_engine.helpers.duckdb import attach_dimension
from data_engine.helpers.duckdb import compact_database
from data_engine.helpers.duckdb import denormalize_columns
from data_engine.helpers.duckdb import normalize_columns
from data_engine.helpers.duckdb import read_rows_by_values
from data_engine.helpers.duckdb import read_sql
from data_engine.helpers.duckdb import read_table
from data_engine.helpers.duckdb import replace_rows_by_file
from data_engine.helpers.duckdb import replace_rows_by_values
from data_engine.helpers.duckdb import replace_table
```

The expected path pattern is:

```python
db_path = context.database("docs/analytics.duckdb")
```

You can also use a mirrored output path or any other DuckDB file path you control. The helpers work with any DuckDB path you provide.

## Shared conventions

The current helper family uses a shared shape:

```python
helper_name(
    db_path,
    table,
    *,
    ...
)
```

Notes:

- `db_path` is positional and required
- `table` is positional and required
- `df` is the incoming Polars dataframe when the helper works on one
- `return_df=True` means "return the dataframe result for this helper"
- identifiers such as table names and column names are quoted safely, including reserved words such as `group`
- schema-qualified tables such as `"mart.fact_claim"` are supported

## `build_dimension(...)`

Use this helper when you already have a dataframe trimmed down to only the natural-key columns and want to persist or extend a surrogate-key table.

Signature:

```python
build_dimension(
    db_path,
    table,
    *,
    df,
    key_column="dimension_key",
    return_df=True,
)
```

Behavior:

- treats every column in `df` as part of the natural key
- creates the table if it does not exist
- inserts only missing unique combinations
- assigns deterministic integer surrogate keys
- returns the natural-key-to-surrogate-key mapping when `return_df=True`

Example:

```python
mapping = build_dimension(
    context.database("warehouse.duckdb"),
    "mart.dim_member",
    df=member_keys_df,
    key_column="member_key",
)
```

Returned mapping:

```text
member_id | lob | member_key
```

## `attach_dimension(...)`

Use this helper when the surrogate-key table already exists and you only want to join the key back onto a dataframe.

Signature:

```python
attach_dimension(
    db_path,
    table,
    *,
    df,
    on,
    key_column="dimension_key",
    drop_key=False,
)
```

Key arguments:

- `on` can be one column name or a list of column names
- `drop_key=False` keeps the natural-key columns by default
- set `drop_key=True` when you want the attached surrogate key without the original key columns

Example:

```python
attached = attach_dimension(
    context.database("warehouse.duckdb"),
    "mart.dim_member",
    df=docs_df,
    on=["member_id", "lob"],
    key_column="member_key",
)
```

## `normalize_columns(...)`

Use this helper when you want to build missing surrogate keys and immediately attach them back onto the full dataframe.

Signature:

```python
normalize_columns(
    db_path,
    table,
    *,
    df,
    on,
    key_column="dimension_key",
    drop_key=True,
    returns="df",
)
```

Key arguments:

- `on` can be one column name or a list of column names
- `drop_key=True` removes the natural-key columns after the surrogate key is joined back
- `returns="df"` returns the normalized dataframe
- `returns="map"` returns only the persisted mapping
- `returns=None` performs side effects only

Example:

```python
normalized = normalize_columns(
    context.database("warehouse.duckdb"),
    "mart.dim_member",
    df=docs_df,
    on=["member_id", "lob"],
    key_column="member_key",
)
```

If `docs_df` starts with:

```text
member_id | lob | amount
```

Then `normalized` becomes:

```text
amount | member_key
```

This helper uses `build_dimension(...)` and `attach_dimension(...)` internally.

## `denormalize_columns(...)`

Use this helper when your dataframe already has a surrogate key and you want to attach the natural columns back from the persisted dimension table.

Signature:

```python
denormalize_columns(
    db_path,
    table,
    *,
    df,
    key_column="dimension_key",
    select="*",
    drop_key=False,
)
```

Key arguments:

- `key_column` is the surrogate key used to join from `df` into the dimension table
- `select="*"` attaches every non-key column from the dimension table
- `select=[...]` lets you attach only a subset of natural columns
- `drop_key=False` keeps the surrogate key by default

Example:

```python
denormalized = denormalize_columns(
    context.database("warehouse.duckdb"),
    "mart.dim_member",
    df=fact_df,
    key_column="member_key",
)
```

## `replace_rows_by_file(...)`

Use this helper when one incoming dataframe represents the full current contents for one source file.

Signature:

```python
replace_rows_by_file(
    db_path,
    table,
    *,
    df,
    file_hash,
    file_hash_column="file_key",
    return_df=True,
)
```

Behavior:

- adds a constant file-hash column to `df`
- creates the table if it does not exist
- expands the table schema when new columns appear
- deletes existing rows for that file hash
- appends the current batch

Example:

```python
updated = replace_rows_by_file(
    context.database("warehouse.duckdb"),
    "canon.claim_rows",
    df=docs_df,
    file_hash=context.metadata["file_hash"],
)
```

This is the usual pattern for canon-style "replace one file slice" loading.

## `replace_rows_by_values(...)`

Use this helper when one incoming dataframe represents the full current contents for one logical value slice.

Signature:

```python
replace_rows_by_values(
    db_path,
    table,
    *,
    df,
    column,
    return_df=True,
)
```

Behavior:

- takes the distinct values from `df[column]`
- deletes existing rows in the target table where `column` matches any of those values
- appends the current batch
- creates and expands the table as needed

Example:

```python
updated = replace_rows_by_values(
    context.database("warehouse.duckdb"),
    "mart.fact_claim",
    df=docs_for_open_status,
    column="status",
)
```

That says: "replace every persisted `status` slice represented by this batch, then insert this batch."

## `compact_database(...)`

Use this helper for explicit maintenance flows when you want to clean one DuckDB
file after a period of ingestion.

Signature:

```python
compact_database(
    db_path,
    *,
    tables=None,
    drop_all_null_columns=True,
    vacuum=True,
)
```

Behavior:

- inspects one or more user tables in the database
- drops columns whose persisted values are entirely null
- preserves at least one column per table
- optionally runs `VACUUM` after schema cleanup
- returns a Polars summary dataframe with dropped-column and file-size metadata

Example:

```python
summary = compact_database(
    context.database("warehouse.duckdb"),
    tables=["mart.fact_claim", "mart.fact_member"],
    vacuum=True,
)
```

This is a good fit for manual maintenance flows where you want a simple
one-liner per database.

## `read_rows_by_values(...)`

Use this helper when you want a small filtered lookup out of DuckDB as a Polars dataframe.

Signature:

```python
read_rows_by_values(
    db_path,
    table,
    *,
    column,
    is_in,
    select,
)
```

Behavior:

- returns rows where `column` matches one of the provided values
- returns only the selected columns
- uses a temporary lookup table internally, which works better than manually assembling long SQL `IN (...)` strings

Example:

```python
existing = read_rows_by_values(
    context.database("warehouse.duckdb"),
    "mart.fact_claim",
    column="claim_id",
    is_in=[1001, 1002, 1003],
    select=["claim_id", "member_key", "amount"],
)
```

## `read_sql(...)`

Use this helper when you already have the exact DuckDB query you want and just need the result as a Polars dataframe.

Signature:

```python
read_sql(
    db_path,
    *,
    sql,
)
```

Example:

```python
result = read_sql(
    context.database("warehouse.duckdb"),
    sql="""
        SELECT claim_id, amount
        FROM mart.fact_claim
        WHERE amount >= 100
    """,
)
```

This is the most direct read helper. If you already know the SQL you want, use this.

## `read_table(...)`

Use this helper when you want a lightweight table reader without writing the whole SQL statement.

Signature:

```python
read_table(
    db_path,
    table,
    *,
    select="*",
    where=None,
    limit=None,
)
```

Example:

```python
result = read_table(
    context.database("warehouse.duckdb"),
    "mart.fact_claim",
    select=["claim_id", "amount"],
    where='"amount" >= 100',
    limit=100,
)
```

This helper is intentionally small:

- `select` can be `"*"` or a list of column names
- `where` is passed through as SQL
- `limit` is optional

## `replace_table(...)`

Use this helper when you want to replace the entire contents of one table with the current dataframe.

Signature:

```python
replace_table(
    db_path,
    table,
    *,
    df,
    return_df=True,
)
```

Behavior:

- creates the table if it does not exist
- expands the table schema when new columns appear
- deletes all existing rows
- inserts the current dataframe

Example:

```python
replace_table(
    context.database("warehouse.duckdb"),
    "mart.current_snapshot",
    df=snapshot_df,
)
```

This is the simplest full-refresh write helper in the current set.

## Design guidance

These helpers are best when:

- the database path is stable
- table ownership is clear
- the dataframe shape is already mostly what you want
- you want predictable transactional behavior

These helpers support common repeated patterns in flow code. Steps that need custom joins, custom window logic, or highly specific query behavior can use plain DuckDB directly.

## When to use direct DuckDB instead

Prefer direct DuckDB code when:

- the operation is highly custom
- you want several SQL statements in one step
- you want full manual control over relation registration, temp tables, or query flow

The helpers remove repeated boilerplate and keep common warehouse-style operations concise.

# Database Methods

There is no first-class database sub-chain. Use DuckDB directly inside step callables, usually with a workspace-local database path from `context.database(...)`.

If you want common warehouse-style shortcuts, see [DuckDB Helpers](duckdb-helpers.md). That helper layer covers several repeated patterns without taking over general SQL authoring.

The exact `context.database(...)` reference lives in the `FlowContext.database`
docstring and is rendered in the API reference. Keep parameter details and
copyable method examples beside the code in `src/data_engine/core/primitives.py`
so editor help and packaged docs stay aligned.

That is intentional. The core API keeps connection ownership, transactions, and query semantics explicit in step code.

In practice, that means:

- Data Engine gives you a conventional path
- your step opens and closes the database connection
- normal DuckDB and Python rules apply

## `context.database(...)`

`context.database(name)` returns a path beneath the current authored workspace's `databases/` folder.

Examples:

```python
context.database("analytics.duckdb")
context.database("docs/analytics.duckdb")
```

Those resolve to:

- `workspaces/<workspace_id>/databases/analytics.duckdb`
- `workspaces/<workspace_id>/databases/docs/analytics.duckdb`

Rules:

- the path must be relative
- parent directories are created automatically
- the helper is only available for authored workspace flows
- it returns the database path for your step to open

That last point is important. Returning the path keeps connection lifetime explicit and easy to reason about.

## Example

```python
import duckdb
import polars as pl

from data_engine import Flow


def read_docs(file_ref):
    return pl.read_excel(file_ref.path)


def build_source(context):
    return pl.concat(context.current, how="vertical_relaxed")


def summarize(context):
    conn = duckdb.connect(context.database("analytics.duckdb"))
    try:
        conn.register("input", context.current)
        return conn.sql(
            """
            select workflow, count(*) as row_count
            from input
            group by workflow
            """
        ).pl()
    finally:
        conn.close()


def build():
    return (
        Flow(group="Analytics")
        .watch(
            mode="schedule",
            run_as="batch",
            interval="15m",
            source="../../example_data/Input/docs_flat",
        )
        .mirror(root="../../example_data/Output/example_summary")
        .collect([".xlsx"], save_as="doc_files")
        .map(read_docs, use="doc_files", save_as="doc_frames")
        .step(build_source, use="doc_frames", save_as="raw_df")
        .step(summarize, use="raw_df", save_as="summary_df")
    )
```

This keeps the flow API small while still letting flow modules use native SQL and native DuckDB connections.

## Good patterns

- open the connection inside the step that needs it
- close the connection in `finally:`
- keep the path stable when you want incremental or append-oriented databases
- use subfolders such as `docs/analytics.duckdb` when one workspace owns several related databases

## A note on mirror vs database paths

If the database is a durable workspace-local asset, prefer `context.database(...)`.

If the database is really just another output artifact produced by one mirrored source flow, `context.mirror.root_file("analytics.duckdb")` can still be appropriate.

The difference is mostly semantic:

- `context.database(...)` says "this belongs to the workspace as a local database"
- `context.mirror...` says "this belongs to this flow's output namespace"

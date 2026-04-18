"""DuckDB helper functions that read persisted data into Polars."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from data_engine.helpers.duckdb import duckdb
from data_engine.helpers.duckdb._common import _normalize_optional_limit
from data_engine.helpers.duckdb._common import _normalize_selected_columns
from data_engine.helpers.duckdb._common import _ordered_columns
from data_engine.helpers.duckdb._common import _quote_identifier
from data_engine.helpers.duckdb._common import _quote_table_ref
from data_engine.helpers.duckdb._common import _resolved_db_path
from data_engine.helpers.duckdb._common import _table_column_names


def read_rows_by_values(
    db_path: str | Path,
    table: str,
    *,
    column: str,
    is_in: list[object] | tuple[object, ...],
    select: str | list[str] | tuple[str, ...],
):
    """Return selected columns for rows whose one column matches provided values.

    Parameters
    ----------
    db_path : str | Path
        DuckDB database file path.
    table : str
        Source table name, optionally schema-qualified.
    column : str
        Column matched against ``is_in``.
    is_in : list[object] | tuple[object, ...]
        Values to include.
    select : str | list[str] | tuple[str, ...]
        Columns to return.

    Returns
    -------
    pl.DataFrame
        Selected matching rows in input order by distinct lookup values.

    Raises
    ------
    ValueError
        If the table, column, or selected columns are invalid.
    """

    normalized_column = str(column).strip()
    if not normalized_column:
        raise ValueError("column must be non-empty.")

    normalized_select = _normalize_selected_columns(select)
    normalized_is_in = tuple(is_in)
    lookup = pl.DataFrame({"lookup_value": list(normalized_is_in)}).unique(maintain_order=True)
    lookup_values = tuple(lookup.get_column("lookup_value").to_list())
    non_null_lookup_values = [value for value in lookup_values if value is not None]
    has_null_lookup = len(non_null_lookup_values) != len(lookup_values)

    quoted_table = _quote_table_ref(table)
    quoted_column = _quote_identifier(normalized_column)
    quoted_select = _ordered_columns(normalized_select)

    temp_lookup = "__data_engine_read_values_lookup"
    temp_lookup_null = "__data_engine_read_values_lookup_null"
    resolved_db_path = _resolved_db_path(db_path)

    connection = duckdb.connect(resolved_db_path)
    try:
        table_columns = _table_column_names(connection, table)
        if not table_columns:
            raise ValueError(f"Table {table!r} does not exist or has no columns.")
        if normalized_column not in table_columns:
            raise ValueError(f"column {normalized_column!r} must exist in table {table!r}.")
        missing_columns = [name for name in normalized_select if name not in table_columns]
        if missing_columns:
            raise ValueError(f"select columns must exist in table {table!r}: {missing_columns!r}")
        if not normalized_is_in:
            return connection.execute(
                f"""
                SELECT {quoted_select}
                FROM {quoted_table}
                WHERE 1 = 0
                """
            ).pl()

        query_parts: list[str] = []
        if non_null_lookup_values:
            connection.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE {temp_lookup} AS
                SELECT *
                FROM (
                    SELECT
                        UNNEST(?) AS lookup_value,
                        ROW_NUMBER() OVER () AS lookup_order
                )
                """
            , [non_null_lookup_values])
            query_parts.append(
                f"""
                SELECT {quoted_select}, lookup.lookup_order AS __lookup_order
                FROM {quoted_table} AS source
                INNER JOIN {temp_lookup} AS lookup
                    ON source.{quoted_column} = lookup.lookup_value
                """
            )
        if has_null_lookup:
            null_order = next(index for index, value in enumerate(lookup_values, start=1) if value is None)
            connection.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE {temp_lookup_null} AS
                SELECT ?::INTEGER AS lookup_order
                """
            , [null_order])
            query_parts.append(
                f"""
                SELECT {quoted_select}, lookup.lookup_order AS __lookup_order
                FROM {quoted_table} AS source
                INNER JOIN {temp_lookup_null} AS lookup
                    ON source.{quoted_column} IS NULL
                """
            )
        return connection.execute(
            "\nUNION ALL\n".join(query_parts) + "\nORDER BY __lookup_order"
        ).pl().drop("__lookup_order")
    finally:
        connection.close()


def read_sql(db_path: str | Path, *, sql: str):
    """Run one SQL query against DuckDB and return the result as a Polars dataframe.

    Parameters
    ----------
    db_path : str | Path
        DuckDB database file path.
    sql : str
        Query text to execute.

    Returns
    -------
    pl.DataFrame
        Query result as a Polars dataframe.
    """

    statement = str(sql).strip()
    if not statement:
        raise ValueError("sql must be non-empty.")

    resolved_db_path = _resolved_db_path(db_path)
    connection = duckdb.connect(resolved_db_path)
    try:
        return connection.execute(statement).pl()
    finally:
        connection.close()


def read_table(
    db_path: str | Path,
    table: str,
    *,
    select: str | list[str] | tuple[str, ...] = "*",
    where: str | None = None,
    limit: int | None = None,
):
    """Read rows from one DuckDB table into a Polars dataframe.

    Parameters
    ----------
    db_path : str | Path
        DuckDB database file path.
    table : str
        Source table name, optionally schema-qualified.
    select : str | list[str] | tuple[str, ...]
        Columns to return, or ``"*"`` for all columns.
    where : str | None
        Optional raw SQL predicate appended after ``WHERE``.
    limit : int | None
        Optional row limit.

    Returns
    -------
    pl.DataFrame
        Selected table rows.
    """

    normalized_limit = _normalize_optional_limit(limit)
    normalized_where = None if where is None else str(where).strip()

    resolved_db_path = _resolved_db_path(db_path)
    connection = duckdb.connect(resolved_db_path)
    try:
        table_columns = _table_column_names(connection, table)
        if not table_columns:
            raise ValueError(f"Table {table!r} does not exist or has no columns.")
    finally:
        connection.close()

    if select == "*":
        quoted_select = "*"
    else:
        selected_columns = _normalize_selected_columns(select)
        missing_columns = [column_name for column_name in selected_columns if column_name not in table_columns]
        if missing_columns:
            raise ValueError(f"select columns must exist in table {table!r}: {missing_columns!r}")
        quoted_select = _ordered_columns(selected_columns)

    query_parts = [f"SELECT {quoted_select}", f"FROM {_quote_table_ref(table)}"]
    if normalized_where:
        query_parts.append(f"WHERE {normalized_where}")
    if normalized_limit is not None:
        query_parts.append(f"LIMIT {normalized_limit}")
    return read_sql(db_path, sql="\n".join(query_parts))

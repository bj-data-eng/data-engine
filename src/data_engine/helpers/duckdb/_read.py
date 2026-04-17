"""DuckDB helper functions that read persisted data into Polars."""

from __future__ import annotations

from pathlib import Path

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

    quoted_table = _quote_table_ref(table)
    quoted_column = _quote_identifier(normalized_column)
    quoted_select = _ordered_columns(normalized_select)

    temp_lookup = "__data_engine_read_values_lookup"
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
        , [list(normalized_is_in)])
        return connection.execute(
            f"""
            SELECT {quoted_select}
            FROM {quoted_table} AS source
            INNER JOIN {temp_lookup} AS lookup
                ON source.{quoted_column} IS NOT DISTINCT FROM lookup.lookup_value
            ORDER BY lookup.lookup_order
            """
        ).pl()
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

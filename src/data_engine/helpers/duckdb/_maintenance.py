"""DuckDB helper functions for explicit database maintenance."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl

from data_engine.helpers.duckdb import duckdb
from data_engine.helpers.duckdb._common import _list_base_tables
from data_engine.helpers.duckdb._common import _normalize_key_columns
from data_engine.helpers.duckdb._common import _normalize_table_names
from data_engine.helpers.duckdb._common import _quote_identifier
from data_engine.helpers.duckdb._common import _quote_table_ref
from data_engine.helpers.duckdb._common import _resolved_db_path
from data_engine.helpers.duckdb._common import _table_column_names


def _default_index_name(*, table: str, columns: tuple[str, ...]) -> str:
    digest = hashlib.sha1(f"{table}|{'|'.join(columns)}".encode("utf-8")).hexdigest()[:12]
    return f"idx_de_{digest}"


def ensure_index(
    db_path: str | Path,
    table: str,
    *,
    columns: str | list[str] | tuple[str, ...],
    name: str | None = None,
) -> str:
    """Create one DuckDB index if it does not already exist.

    Parameters
    ----------
    db_path : str | Path
        DuckDB database file path.
    table : str
        Target table name, optionally schema-qualified.
    columns : str | list[str] | tuple[str, ...]
        Column or columns to index.
    name : str | None
        Optional index name. When omitted, Data Engine generates a stable name
        from the table and columns.

    Returns
    -------
    str
        Index name that exists after the call.

    Raises
    ------
    ValueError
        If the table does not exist, selected columns do not exist, or the
        provided index name is empty.

    Examples
    --------
    Index a file-slice column before repeated ``replace_rows_by_file`` calls:

    .. code-block:: python

        data_engine.helpers.duckdb.ensure_index(
            context.database("warehouse.duckdb"),
            "fact_claim",
            columns="file_key",
        )

    Index a lookup column before repeated ``read_rows_by_values`` calls:

    .. code-block:: python

        data_engine.helpers.duckdb.ensure_index(
            context.database("warehouse.duckdb"),
            "fact_claim",
            columns="claim_id",
            name="idx_fact_claim_claim_id",
        )
    """
    normalized_columns = _normalize_key_columns(columns)
    if name is None:
        index_name = _default_index_name(table=table, columns=normalized_columns)
    else:
        index_name = str(name).strip()
        if not index_name:
            raise ValueError("name must be non-empty.")

    resolved_db_path = _resolved_db_path(db_path)
    connection = duckdb.connect(resolved_db_path)
    try:
        try:
            table_columns = _table_column_names(connection, table)
        except duckdb.CatalogException as exc:
            raise ValueError(f"Table {table!r} does not exist or has no columns.") from exc
        if not table_columns:
            raise ValueError(f"Table {table!r} does not exist or has no columns.")
        missing_columns = [column for column in normalized_columns if column not in table_columns]
        if missing_columns:
            raise ValueError(f"columns must exist in table {table!r}: {missing_columns!r}")
        connection.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {_quote_identifier(index_name)}
            ON {_quote_table_ref(table)} ({", ".join(_quote_identifier(column) for column in normalized_columns)})
            """
        )
    finally:
        connection.close()
    return index_name


def compact_database(
    db_path: str | Path,
    *,
    tables: str | list[str] | tuple[str, ...] | None = None,
    drop_all_null_columns: bool = True,
    vacuum: bool = True,
) -> pl.DataFrame:
    """Compact one DuckDB database by dropping all-null columns and optionally vacuuming."""

    normalized_tables = _normalize_table_names(tables)
    resolved_db_path = _resolved_db_path(db_path)
    size_before_bytes = resolved_db_path.stat().st_size if resolved_db_path.exists() else 0

    connection = duckdb.connect(resolved_db_path)
    try:
        available_tables = _list_base_tables(connection)
        if normalized_tables is None:
            target_tables = available_tables
        else:
            missing_tables = [table for table in normalized_tables if table not in available_tables]
            if missing_tables:
                raise ValueError(f"tables must exist in database: {missing_tables!r}")
            target_tables = normalized_tables

        summary_rows: list[dict[str, object]] = []
        connection.execute("BEGIN TRANSACTION")
        try:
            for table_name in target_tables:
                table_columns = list(_table_column_names(connection, table_name))
                dropped_columns: list[str] = []

                if drop_all_null_columns and len(table_columns) > 1:
                    nullable_candidates: list[str] = []
                    quoted_table = _quote_table_ref(table_name)
                    for column_name in table_columns:
                        quoted_column = _quote_identifier(column_name)
                        has_non_null = connection.execute(
                            f"""
                            SELECT 1
                            FROM {quoted_table}
                            WHERE {quoted_column} IS NOT NULL
                            LIMIT 1
                            """
                        ).fetchone()
                        if has_non_null is None:
                            nullable_candidates.append(column_name)

                    if len(nullable_candidates) >= len(table_columns):
                        nullable_candidates = nullable_candidates[1:]

                    for column_name in nullable_candidates:
                        connection.execute(
                            f"ALTER TABLE {quoted_table} DROP COLUMN {_quote_identifier(column_name)}"
                        )
                        dropped_columns.append(column_name)

                summary_rows.append(
                    {
                        "db_path": str(resolved_db_path),
                        "table": table_name,
                        "dropped_column_count": len(dropped_columns),
                        "dropped_columns": dropped_columns,
                        "vacuum_requested": vacuum,
                    }
                )

            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
            raise

        vacuumed = False
        if vacuum:
            connection.execute("VACUUM")
            vacuumed = True
    finally:
        connection.close()

    size_after_bytes = resolved_db_path.stat().st_size if resolved_db_path.exists() else 0
    return pl.DataFrame(
        {
            "db_path": [row["db_path"] for row in summary_rows],
            "table": [row["table"] for row in summary_rows],
            "dropped_column_count": [row["dropped_column_count"] for row in summary_rows],
            "dropped_columns": [row["dropped_columns"] for row in summary_rows],
            "vacuum_requested": [row["vacuum_requested"] for row in summary_rows],
            "vacuumed": [vacuumed for _ in summary_rows],
            "size_before_bytes": [size_before_bytes for _ in summary_rows],
            "size_after_bytes": [size_after_bytes for _ in summary_rows],
        }
    )

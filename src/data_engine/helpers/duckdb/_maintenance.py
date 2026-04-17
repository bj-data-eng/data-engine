"""DuckDB helper functions for explicit database maintenance."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from data_engine.helpers.duckdb import duckdb
from data_engine.helpers.duckdb._common import _list_base_tables
from data_engine.helpers.duckdb._common import _normalize_table_names
from data_engine.helpers.duckdb._common import _quote_identifier
from data_engine.helpers.duckdb._common import _quote_table_ref
from data_engine.helpers.duckdb._common import _resolved_db_path
from data_engine.helpers.duckdb._common import _table_column_names


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

"""DuckDB helper functions that replace persisted table slices."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from data_engine.helpers.duckdb import duckdb
from data_engine.helpers.duckdb._common import FrameLike
from data_engine.helpers.duckdb._common import _existing_table_columns
from data_engine.helpers.duckdb._common import _materialize_frame
from data_engine.helpers.duckdb._common import _ordered_columns
from data_engine.helpers.duckdb._common import _quote_identifier
from data_engine.helpers.duckdb._common import _quote_table_ref
from data_engine.helpers.duckdb._common import _resolved_db_path
from data_engine.helpers.duckdb._common import _schema_ref


def _relation_columns(connection, relation: str) -> list[tuple[str, str]]:
    return [
        (str(name), str(dtype))
        for name, dtype, *_ in connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    ]


def _create_or_extend_table_from_relation(
    connection,
    *,
    table: str,
    quoted_table: str,
    relation: str,
) -> dict[str, str]:
    connection.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} AS SELECT * FROM {relation} WHERE 1 = 0")
    existing_columns = {name: dtype for _, name, dtype, *_ in _existing_table_columns(connection, table)}
    for name, dtype in _relation_columns(connection, relation):
        if name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {quoted_table} ADD COLUMN {_quote_identifier(name)} {dtype}")
        existing_columns[name] = dtype
    return existing_columns


def replace_rows_by_file(
    db_path: str | Path,
    table: str,
    *,
    df: FrameLike,
    file_hash: str,
    file_hash_column: str = "file_key",
    return_df: bool = True,
):
    """Atomically replace one file's fact rows and append the current batch."""

    df = _materialize_frame(df)

    normalized_file_hash = str(file_hash).strip()
    if not normalized_file_hash:
        raise ValueError("file_hash must be non-empty.")

    normalized_file_hash_column = str(file_hash_column).strip()
    if not normalized_file_hash_column:
        raise ValueError("file_hash_column must be non-empty.")
    if normalized_file_hash_column in df.columns:
        raise ValueError(f'file_hash_column {normalized_file_hash_column!r} must not already exist in df columns.')

    incoming_with_hash = df.with_columns(pl.lit(normalized_file_hash).alias(normalized_file_hash_column))
    incoming_columns = tuple(incoming_with_hash.columns)
    if not incoming_columns:
        raise ValueError("df must include at least one column.")

    quoted_table = _quote_table_ref(table)
    quoted_schema = _schema_ref(table)
    quoted_file_hash_column = _quote_identifier(normalized_file_hash_column)
    quoted_incoming_columns = _ordered_columns(incoming_columns)

    temp_view = "__data_engine_incremental_incoming"

    resolved_db_path = _resolved_db_path(db_path)

    connection = duckdb.connect(resolved_db_path)
    try:
        connection.execute("BEGIN TRANSACTION")
        if quoted_schema is not None:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")

        connection.register(temp_view, incoming_with_hash)
        _create_or_extend_table_from_relation(
            connection,
            table=table,
            quoted_table=quoted_table,
            relation=temp_view,
        )

        connection.execute(
            f"""
            DELETE FROM {quoted_table}
            WHERE {quoted_file_hash_column} = ?
            """,
            [normalized_file_hash],
        )
        connection.execute(
            f"""
            INSERT INTO {quoted_table} ({quoted_incoming_columns})
            SELECT {quoted_incoming_columns}
            FROM {temp_view}
            """
        )

        if not return_df:
            connection.execute("COMMIT")
            return None

        connection.execute("COMMIT")
        return incoming_with_hash
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()


def replace_rows_by_values(
    db_path: str | Path,
    table: str,
    *,
    df: FrameLike,
    column: str,
    return_df: bool = True,
):
    """Atomically replace one value-slice of rows and append the current batch."""

    df = _materialize_frame(df)
    if df.is_empty():
        raise ValueError("df must include at least one row.")

    normalized_column = str(column).strip()
    if not normalized_column:
        raise ValueError("column must be non-empty.")
    if normalized_column not in df.columns:
        raise ValueError(f'column {normalized_column!r} must exist in df columns.')

    quoted_table = _quote_table_ref(table)
    quoted_schema = _schema_ref(table)
    quoted_column = _quote_identifier(normalized_column)
    quoted_df_columns = _ordered_columns(tuple(df.columns))

    temp_view = "__data_engine_replace_values_df"
    temp_lookup_table = "__data_engine_replace_values_lookup_table"

    resolved_db_path = _resolved_db_path(db_path)

    connection = duckdb.connect(resolved_db_path)
    try:
        connection.execute("BEGIN TRANSACTION")
        if quoted_schema is not None:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")

        connection.register(temp_view, df)
        existing_columns = _create_or_extend_table_from_relation(
            connection,
            table=table,
            quoted_table=quoted_table,
            relation=temp_view,
        )

        target_column_type = existing_columns[normalized_column]

        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {temp_lookup_table} AS
            SELECT DISTINCT CAST({quoted_column} AS {target_column_type}) AS lookup_value
            FROM {temp_view}
            """
        )
        connection.execute(
            f"""
            DELETE FROM {quoted_table} AS target
            USING {temp_lookup_table} AS lookup
            WHERE target.{quoted_column} IS NOT DISTINCT FROM lookup.lookup_value
            """
        )
        connection.execute(
            f"""
            INSERT INTO {quoted_table} ({quoted_df_columns})
            SELECT {quoted_df_columns}
            FROM {temp_view}
            """
        )

        if not return_df:
            connection.execute("COMMIT")
            return None

        connection.execute("COMMIT")
        return df
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()


def replace_table(
    db_path: str | Path,
    table: str,
    *,
    df: FrameLike,
    return_df: bool = True,
):
    """Replace one DuckDB table wholesale from the provided dataframe."""

    df = _materialize_frame(df)
    df_columns = tuple(df.columns)
    if not df_columns:
        raise ValueError("df must include at least one column.")

    quoted_table = _quote_table_ref(table)
    quoted_schema = _schema_ref(table)
    quoted_df_columns = _ordered_columns(df_columns)

    temp_view = "__data_engine_replace_table_df"
    temp_table = "__data_engine_replace_table_df_table"

    resolved_db_path = _resolved_db_path(db_path)

    connection = duckdb.connect(resolved_db_path)
    try:
        connection.execute("BEGIN TRANSACTION")
        if quoted_schema is not None:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")

        connection.register(temp_view, df)
        connection.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} AS SELECT * FROM {temp_view}")
        connection.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} AS SELECT * FROM {temp_table} WHERE 1 = 0")

        existing_columns = {name: dtype for _, name, dtype, *_ in _existing_table_columns(connection, table)}
        incoming_info = connection.execute(f"PRAGMA table_info({temp_table})").fetchall()
        for _, name, dtype, *_ in incoming_info:
            if name in existing_columns:
                continue
            connection.execute(f"ALTER TABLE {quoted_table} ADD COLUMN {_quote_identifier(name)} {dtype}")

        connection.execute(f"DELETE FROM {quoted_table}")
        connection.execute(
            f"""
            INSERT INTO {quoted_table} ({quoted_df_columns})
            SELECT {quoted_df_columns}
            FROM {temp_table}
            """
        )

        if not return_df:
            connection.execute("COMMIT")
            return None

        connection.execute("COMMIT")
        return df
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()

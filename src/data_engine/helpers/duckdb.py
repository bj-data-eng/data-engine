"""Public one-shot DuckDB helpers for flow authoring."""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import polars as pl


def _quote_identifier(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("Identifier must be non-empty.")
    return '"' + text.replace('"', '""') + '"'


def _quote_table_ref(value: str) -> str:
    parts = [part.strip() for part in str(value).split(".")]
    if not parts or any(not part for part in parts):
        raise ValueError("Table name must be non-empty.")
    return ".".join(_quote_identifier(part) for part in parts)


def _schema_ref(value: str) -> str | None:
    parts = [part.strip() for part in str(value).split(".")]
    if len(parts) <= 1:
        return None
    return ".".join(_quote_identifier(part) for part in parts[:-1])


def _join_predicate(*, left_alias: str, right_alias: str, columns: tuple[str, ...]) -> str:
    return " AND ".join(
        f'{left_alias}.{_quote_identifier(column)} IS NOT DISTINCT FROM {right_alias}.{_quote_identifier(column)}'
        for column in columns
    )


def _ordered_columns(columns: tuple[str, ...]) -> str:
    return ", ".join(_quote_identifier(column) for column in columns)


def _qualified_columns(alias: str, columns: tuple[str, ...]) -> str:
    return ", ".join(f"{alias}.{_quote_identifier(column)}" for column in columns)


def _index_name(*, table: str, columns: tuple[str, ...]) -> str:
    digest = hashlib.sha1(f"{table}|{'|'.join(columns)}".encode("utf-8")).hexdigest()[:10]
    return f"uq_dim_{digest}"


def _existing_table_columns(connection, table: str) -> list[tuple[int, str, str, bool, object, bool]]:
    schema = _schema_ref(table)
    table_name = str(table).split(".")[-1].strip()
    if schema is None:
        return connection.execute(f"PRAGMA table_info({_quote_identifier(table_name)})").fetchall()
    return connection.execute(f"PRAGMA table_info({_quote_table_ref(table)})").fetchall()


def _table_column_names(connection, table: str) -> tuple[str, ...]:
    return tuple(name for _, name, *_ in _existing_table_columns(connection, table))


def _normalize_selected_columns(select: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(select, str):
        normalized = (select.strip(),)
    else:
        normalized = tuple(str(value).strip() for value in select)
    if not normalized or any(not value for value in normalized):
        raise ValueError("select must include at least one non-empty column name.")
    return normalized


def _normalize_key_columns(on: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(on, str):
        normalized = (on.strip(),)
    else:
        normalized = tuple(str(value).strip() for value in on)
    if not normalized or any(not value for value in normalized):
        raise ValueError("on must include at least one non-empty column name.")
    return normalized


def _normalize_optional_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    normalized = int(limit)
    if normalized < 0:
        raise ValueError("limit must be non-negative.")
    return normalized


def build_dimension(
    db_path: str | Path,
    table: str,
    *,
    df: pl.DataFrame,
    key_column: str = "dimension_key",
    return_df: bool = True,
) -> pl.DataFrame | None:
    """Build or extend one dimension table from unique incoming row combinations.

    The incoming dataframe is treated as the natural key definition: every incoming
    column participates in uniqueness. The helper ensures the dimension table
    exists, inserts only new combinations, assigns deterministic surrogate keys,
    and optionally returns the natural-key-to-surrogate-key mapping.
    """

    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a Polars DataFrame.")

    natural_columns = tuple(df.columns)
    if not natural_columns:
        raise ValueError("df must include at least one column.")

    normalized_key_column = str(key_column).strip()
    if not normalized_key_column:
        raise ValueError("key_column must be non-empty.")
    if normalized_key_column in natural_columns:
        raise ValueError(f'key_column {normalized_key_column!r} must not already exist in df columns.')

    quoted_table = _quote_table_ref(table)
    quoted_schema = _schema_ref(table)
    quoted_key_column = _quote_identifier(normalized_key_column)
    quoted_natural_columns = _ordered_columns(natural_columns)
    qualified_mapping_columns = _qualified_columns("mapping", natural_columns)
    natural_join = _join_predicate(left_alias="candidate", right_alias="existing", columns=natural_columns)
    mapping_join = _join_predicate(left_alias="mapping", right_alias="incoming_distinct", columns=natural_columns)
    order_by_columns = quoted_natural_columns

    temp_view = "__data_engine_dimension_incoming"
    temp_distinct = "__data_engine_dimension_incoming_distinct"
    temp_new_rows = "__data_engine_dimension_new_rows"
    unique_index_name = _quote_identifier(_index_name(table=table, columns=natural_columns))

    resolved_db_path = Path(db_path).expanduser().resolve()
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(resolved_db_path)
    try:
        connection.execute("BEGIN TRANSACTION")
        if quoted_schema is not None:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
        connection.register(temp_view, df)
        connection.execute(f"CREATE OR REPLACE TEMP TABLE {temp_distinct} AS SELECT DISTINCT * FROM {temp_view}")
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {quoted_table} AS
            SELECT
                CAST(NULL AS BIGINT) AS {quoted_key_column},
                *
            FROM {temp_distinct}
            WHERE 1 = 0
            """
        )
        connection.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {unique_index_name} ON {quoted_table} ({quoted_natural_columns})"
        )
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {temp_new_rows} AS
            SELECT candidate.*
            FROM {temp_distinct} AS candidate
            LEFT JOIN {quoted_table} AS existing
                ON {natural_join}
            WHERE existing.{quoted_key_column} IS NULL
            """
        )
        connection.execute(
            f"""
            INSERT INTO {quoted_table} ({quoted_key_column}, {quoted_natural_columns})
            SELECT
                current_keys.max_existing_key + ROW_NUMBER() OVER (ORDER BY {order_by_columns}) AS {quoted_key_column},
                new_rows.*
            FROM {temp_new_rows} AS new_rows
            CROSS JOIN (
                SELECT COALESCE(MAX({quoted_key_column}), 0) AS max_existing_key
                FROM {quoted_table}
            ) AS current_keys
            """
        )

        if not return_df:
            connection.execute("COMMIT")
            return None

        mapping = connection.execute(
            f"""
            SELECT {qualified_mapping_columns}, mapping.{quoted_key_column}
            FROM {quoted_table} AS mapping
            INNER JOIN {temp_distinct} AS incoming_distinct
                ON {mapping_join}
            ORDER BY {order_by_columns}
            """
        ).pl()
        connection.execute("COMMIT")
        return mapping
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()


def replace_rows_by_file(
    db_path: str | Path,
    table: str,
    *,
    df: pl.DataFrame,
    file_hash: str,
    file_hash_column: str = "file_key",
    return_df: bool = True,
) -> pl.DataFrame | None:
    """Atomically replace one file's fact rows and append the current batch."""

    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a Polars DataFrame.")

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
    temp_table = "__data_engine_incremental_incoming_table"

    resolved_db_path = Path(db_path).expanduser().resolve()
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(resolved_db_path)
    try:
        connection.execute("BEGIN TRANSACTION")
        if quoted_schema is not None:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
        connection.register(temp_view, incoming_with_hash)
        connection.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} AS SELECT * FROM {temp_view}")
        connection.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} AS SELECT * FROM {temp_table} WHERE 1 = 0")

        existing_columns = {name: dtype for _, name, dtype, *_ in _existing_table_columns(connection, table)}
        incoming_info = connection.execute(f"PRAGMA table_info({temp_table})").fetchall()
        for _, name, dtype, *_ in incoming_info:
            if name in existing_columns:
                continue
            connection.execute(f"ALTER TABLE {quoted_table} ADD COLUMN {_quote_identifier(name)} {dtype}")

        connection.execute(
            f"DELETE FROM {quoted_table} WHERE {quoted_file_hash_column} = ?",
            [normalized_file_hash],
        )
        connection.execute(
            f"""
            INSERT INTO {quoted_table} ({quoted_incoming_columns})
            SELECT {quoted_incoming_columns}
            FROM {temp_table}
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
    df: pl.DataFrame,
    column: str,
    return_df: bool = True,
) -> pl.DataFrame | None:
    """Atomically replace one value-slice of rows and append the current batch."""

    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a Polars DataFrame.")
    if df.is_empty():
        raise ValueError("df must include at least one row.")

    normalized_column = str(column).strip()
    if not normalized_column:
        raise ValueError("column must be non-empty.")
    if normalized_column not in df.columns:
        raise ValueError(f'column {normalized_column!r} must exist in df columns.')

    lookup = df.select(pl.col(normalized_column)).unique(maintain_order=True)
    if lookup.is_empty():
        raise ValueError("df must include at least one replacement value.")

    quoted_table = _quote_table_ref(table)
    quoted_schema = _schema_ref(table)
    quoted_column = _quote_identifier(normalized_column)
    quoted_df_columns = _ordered_columns(tuple(df.columns))

    temp_view = "__data_engine_replace_values_df"
    temp_table = "__data_engine_replace_values_df_table"
    temp_lookup_view = "__data_engine_replace_values_lookup"
    temp_lookup_table = "__data_engine_replace_values_lookup_table"

    resolved_db_path = Path(db_path).expanduser().resolve()
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

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

        connection.register(temp_lookup_view, lookup)
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {temp_lookup_table} AS
            SELECT {_quote_identifier(normalized_column)} AS lookup_value
            FROM {temp_lookup_view}
            """
        )

        connection.execute(
            f"""
            DELETE FROM {quoted_table}
            WHERE {quoted_column} IN (
                SELECT lookup_value
                FROM {temp_lookup_table}
            )
            """
        )
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


def attach_dimension(
    db_path: str | Path,
    table: str,
    *,
    df: pl.DataFrame,
    on: str | list[str] | tuple[str, ...],
    key_column: str = "dimension_key",
    drop_key: bool = False,
) -> pl.DataFrame:
    """Attach an existing surrogate key mapping table to an input dataframe."""

    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a Polars DataFrame.")

    join_columns = _normalize_key_columns(on)
    missing_columns = [column for column in join_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"on columns must exist in df: {missing_columns!r}")

    mapping = read_rows_by_values(
        db_path,
        table,
        column=join_columns[0],
        is_in=df.get_column(join_columns[0]).unique().to_list(),
        select=[*join_columns, key_column],
    ).unique(subset=list(join_columns), maintain_order=True)

    normalized = df.join(mapping, on=list(join_columns), how="left", validate="m:1")
    if drop_key:
        normalized = normalized.drop(list(join_columns))
    return normalized


def denormalize_columns(
    db_path: str | Path,
    table: str,
    *,
    df: pl.DataFrame,
    key_column: str = "dimension_key",
    select: str | list[str] | tuple[str, ...] = "*",
    drop_key: bool = False,
) -> pl.DataFrame:
    """Attach natural columns from an existing dimension table onto a keyed dataframe."""

    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a Polars DataFrame.")

    normalized_key_column = str(key_column).strip()
    if not normalized_key_column:
        raise ValueError("key_column must be non-empty.")
    if normalized_key_column not in df.columns:
        raise ValueError(f"key_column {normalized_key_column!r} must exist in df.")

    resolved_db_path = Path(db_path).expanduser().resolve()
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(resolved_db_path)
    try:
        table_columns = _table_column_names(connection, table)
    finally:
        connection.close()

    if not table_columns:
        raise ValueError(f"Table {table!r} does not exist or has no columns.")
    if normalized_key_column not in table_columns:
        raise ValueError(f"key_column {normalized_key_column!r} must exist in table {table!r}.")

    if select == "*":
        selected_columns = tuple(column for column in table_columns if column != normalized_key_column)
    else:
        selected_columns = _normalize_selected_columns(select)
        missing_columns = [column for column in selected_columns if column not in table_columns]
        if missing_columns:
            raise ValueError(f"select columns must exist in table {table!r}: {missing_columns!r}")
        if normalized_key_column in selected_columns:
            raise ValueError(f"select must not include key_column {normalized_key_column!r}.")

    if not selected_columns:
        raise ValueError("select must include at least one non-key column.")

    mapping = read_rows_by_values(
        db_path,
        table,
        column=normalized_key_column,
        is_in=df.get_column(normalized_key_column).unique().to_list(),
        select=[normalized_key_column, *selected_columns],
    ).unique(subset=[normalized_key_column], maintain_order=True)

    denormalized = df.join(mapping, on=[normalized_key_column], how="left", validate="m:1")
    if drop_key:
        denormalized = denormalized.drop([normalized_key_column])
    return denormalized


def normalize_columns(
    db_path: str | Path,
    table: str,
    *,
    df: pl.DataFrame,
    on: str | list[str] | tuple[str, ...],
    key_column: str = "dimension_key",
    drop_key: bool = True,
    returns: str | None = "df",
) -> pl.DataFrame | None:
    """Build missing surrogate keys and attach them back onto the input dataframe."""

    if returns not in {"df", "map", None}:
        raise ValueError('returns must be "df", "map", or None.')

    join_columns = _normalize_key_columns(on)
    natural_key_df = df.select(list(join_columns)).unique(maintain_order=True)
    mapping = build_dimension(
        db_path,
        table,
        df=natural_key_df,
        key_column=key_column,
        return_df=True,
    )
    if mapping is None:
        raise RuntimeError("build_dimension() unexpectedly returned no mapping.")

    if returns == "map":
        return mapping
    if returns is None:
        return None

    return attach_dimension(
        db_path,
        table,
        df=df,
        on=join_columns,
        key_column=key_column,
        drop_key=drop_key,
    )


def read_rows_by_values(
    db_path: str | Path,
    table: str,
    *,
    column: str,
    is_in: list[object] | tuple[object, ...],
    select: str | list[str] | tuple[str, ...],
) -> pl.DataFrame:
    """Return selected columns for rows whose one column matches any provided value."""

    normalized_column = str(column).strip()
    if not normalized_column:
        raise ValueError("column must be non-empty.")
    selected_columns = _normalize_selected_columns(select)

    quoted_table = _quote_table_ref(table)
    quoted_column = _quote_identifier(normalized_column)
    selected_sql = _qualified_columns("source_rows", selected_columns)

    resolved_db_path = Path(db_path).expanduser().resolve()
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(resolved_db_path)
    try:
        if not is_in:
            return connection.execute(
                f"""
                SELECT {selected_sql}
                FROM {quoted_table} AS source_rows
                WHERE 1 = 0
                """
            ).pl()

        lookup = pl.DataFrame({"lookup_value": list(is_in)}).unique(maintain_order=True)
        connection.register("__data_engine_lookup_values", lookup)
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE __data_engine_lookup_values_table AS SELECT * FROM __data_engine_lookup_values"
        )
        return connection.execute(
            f"""
            SELECT {selected_sql}
            FROM {quoted_table} AS source_rows
            INNER JOIN __data_engine_lookup_values_table AS lookup
                ON source_rows.{quoted_column} IS NOT DISTINCT FROM lookup.lookup_value
            """
        ).pl()
    finally:
        connection.close()


def read_sql(
    db_path: str | Path,
    *,
    sql: str,
) -> pl.DataFrame:
    """Run one SQL query and return the result as a Polars DataFrame."""

    normalized_sql = str(sql).strip()
    if not normalized_sql:
        raise ValueError("sql must be non-empty.")

    resolved_db_path = Path(db_path).expanduser().resolve()
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(resolved_db_path)
    try:
        return connection.execute(normalized_sql).pl()
    finally:
        connection.close()


def read_table(
    db_path: str | Path,
    table: str,
    *,
    select: str | list[str] | tuple[str, ...] = "*",
    where: str | None = None,
    limit: int | None = None,
) -> pl.DataFrame:
    """Read rows from one table with optional column selection, filter, and limit."""

    quoted_table = _quote_table_ref(table)
    normalized_where = None if where is None else str(where).strip()
    normalized_limit = _normalize_optional_limit(limit)

    if select == "*":
        selected_sql = "*"
    else:
        selected_columns = _normalize_selected_columns(select)
        selected_sql = _ordered_columns(selected_columns)

    query_parts = [f"SELECT {selected_sql}", f"FROM {quoted_table}"]
    if normalized_where:
        query_parts.append(f"WHERE {normalized_where}")
    if normalized_limit is not None:
        query_parts.append(f"LIMIT {normalized_limit}")

    return read_sql(db_path, sql="\n".join(query_parts))


def replace_table(
    db_path: str | Path,
    table: str,
    *,
    df: pl.DataFrame,
    return_df: bool = True,
) -> pl.DataFrame | None:
    """Replace one table wholesale from a dataframe, expanding to the current df schema."""

    if not isinstance(df, pl.DataFrame):
        raise TypeError("df must be a Polars DataFrame.")

    df_columns = tuple(df.columns)
    if not df_columns:
        raise ValueError("df must include at least one column.")

    quoted_table = _quote_table_ref(table)
    quoted_schema = _schema_ref(table)
    quoted_df_columns = _ordered_columns(df_columns)

    temp_view = "__data_engine_replace_table_df"
    temp_table = "__data_engine_replace_table_df_table"

    resolved_db_path = Path(db_path).expanduser().resolve()
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

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


__all__ = [
    "attach_dimension",
    "build_dimension",
    "denormalize_columns",
    "normalize_columns",
    "read_rows_by_values",
    "read_sql",
    "read_table",
    "replace_rows_by_file",
    "replace_rows_by_values",
    "replace_table",
]

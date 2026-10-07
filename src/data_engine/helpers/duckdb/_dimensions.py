"""DuckDB helper functions for dimension-style workflows."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from data_engine.helpers.duckdb import duckdb
from data_engine.helpers.duckdb._common import FrameLike
from data_engine.helpers.duckdb._common import _index_name
from data_engine.helpers.duckdb._common import _join_predicate
from data_engine.helpers.duckdb._common import _materialize_frame
from data_engine.helpers.duckdb._common import _normalize_key_columns
from data_engine.helpers.duckdb._common import _normalize_selected_columns
from data_engine.helpers.duckdb._common import _ordered_columns
from data_engine.helpers.duckdb._common import _qualified_columns
from data_engine.helpers.duckdb._common import _quote_identifier
from data_engine.helpers.duckdb._common import _quote_table_ref
from data_engine.helpers.duckdb._common import _resolved_db_path
from data_engine.helpers.duckdb._common import _schema_ref
from data_engine.helpers.duckdb._common import _selects_all_columns
from data_engine.helpers.duckdb._common import _table_column_names
from data_engine.helpers.duckdb._read import read_rows_by_values


def build_dimension(
    db_path: str | Path,
    table: str,
    *,
    df: FrameLike,
    key_column: str = "dimension_key",
    return_df: bool = True,
):
    """Build or extend one dimension table from unique incoming row combinations."""

    df = _materialize_frame(df)

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

    resolved_db_path = _resolved_db_path(db_path)
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


def attach_dimension(
    db_path: str | Path,
    table: str,
    *,
    df: FrameLike,
    on: str | list[str] | tuple[str, ...],
    key_column: str = "dimension_key",
    drop_key: bool = False,
):
    """Attach an existing surrogate key mapping table to an input dataframe.

    Natural-key components match nulls to nulls, including composite keys.
    The result preserves input row order and count, including repeated keys;
    unmatched keys receive a null surrogate key. ``drop_key=True`` removes
    the natural-key columns after attachment.
    """

    df = _materialize_frame(df)

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

    normalized = df.join(
        mapping, on=list(join_columns), how="left", validate="m:1", nulls_equal=True, maintain_order="left"
    )
    if drop_key:
        normalized = normalized.drop(list(join_columns))
    return normalized


def denormalize_columns(
    db_path: str | Path,
    table: str,
    *,
    df: FrameLike,
    key_column: str = "dimension_key",
    select: str | list[str] | tuple[str, ...] = "*",
    drop_key: bool = False,
):
    """Attach natural columns from an existing dimension table onto a keyed dataframe."""

    df = _materialize_frame(df)

    normalized_key_column = str(key_column).strip()
    if not normalized_key_column:
        raise ValueError("key_column must be non-empty.")
    if normalized_key_column not in df.columns:
        raise ValueError(f"key_column {normalized_key_column!r} must exist in df.")

    resolved_db_path = _resolved_db_path(db_path)

    connection = duckdb.connect(resolved_db_path)
    try:
        table_columns = _table_column_names(connection, table)
    finally:
        connection.close()

    if not table_columns:
        raise ValueError(f"Table {table!r} does not exist or has no columns.")
    if normalized_key_column not in table_columns:
        raise ValueError(f"key_column {normalized_key_column!r} must exist in table {table!r}.")

    if _selects_all_columns(select):
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

    denormalized = df.join(
        mapping, on=[normalized_key_column], how="left", validate="m:1", maintain_order="left"
    )
    if drop_key:
        denormalized = denormalized.drop([normalized_key_column])
    return denormalized


def normalize_columns(
    db_path: str | Path,
    table: str,
    *,
    df: FrameLike,
    on: str | list[str] | tuple[str, ...],
    key_column: str = "dimension_key",
    drop_key: bool = True,
    returns: str | None = "df",
) -> pl.DataFrame | None:
    """Build missing surrogate keys and attach them back onto the input dataframe.

    Each distinct natural-key combination, including null components, receives
    a persistent surrogate key. Repeated calls reuse existing keys. The default
    ``returns="df"`` preserves input row order and count and removes natural-key
    columns when ``drop_key=True``. Use ``returns="map"`` for the distinct
    mapping or ``returns=None`` to persist it without returning a dataframe.

    Parameters
    ----------
    db_path : str | Path
        DuckDB database file path.
    table : str
        Dimension table name, optionally schema-qualified.
    df : FrameLike
        Input dataframe or lazy frame to collect and normalize.
    on : str | list[str] | tuple[str, ...]
        Natural-key columns, matched with null-safe equality.
    key_column : str
        Surrogate key column to create and attach.
    drop_key : bool
        Whether to remove natural-key columns from normalized input rows.
    returns : str | None
        ``"df"`` for normalized input rows, ``"map"`` for the distinct mapping,
        or ``None`` to persist the dimension without returning rows.

    Returns
    -------
    pl.DataFrame | None
        Collected normalized dataframe, mapping dataframe, or ``None`` as
        selected by ``returns``.

    Raises
    ------
    ValueError
        If ``returns`` is unsupported or the natural/key columns are invalid.
    RuntimeError
        If dimension creation unexpectedly fails to return a mapping.

    Examples
    --------
    .. code-block:: python

        import polars as pl
        from data_engine.helpers.duckdb import normalize_columns

        normalized = normalize_columns(
            "output/report.duckdb",
            "dim_member",
            df=pl.DataFrame({"member": ["a", None, "a"], "amount": [10, 20, 30]}),
            on="member",
            key_column="member_key",
        )
    """

    if returns not in {"df", "map", None}:
        raise ValueError('returns must be "df", "map", or None.')

    df = _materialize_frame(df)
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

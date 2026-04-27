"""Shared utilities for the public DuckDB helper package."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl

FrameLike = pl.DataFrame | pl.LazyFrame


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


def _normalize_table_names(tables: str | list[str] | tuple[str, ...] | None) -> tuple[str, ...] | None:
    if tables is None:
        return None
    if isinstance(tables, str):
        normalized = (tables.strip(),)
    else:
        normalized = tuple(str(value).strip() for value in tables)
    if not normalized or any(not value for value in normalized):
        raise ValueError("tables must include at least one non-empty table name.")
    return normalized


def _list_base_tables(connection) -> tuple[str, ...]:
    rows = connection.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
        """
    ).fetchall()
    tables: list[str] = []
    for schema, table_name in rows:
        if str(schema) == "main":
            tables.append(str(table_name))
        else:
            tables.append(f"{schema}.{table_name}")
    return tuple(tables)


def _normalize_selected_columns(select: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(select, str):
        normalized = (select.strip(),)
    else:
        normalized = tuple(str(value).strip() for value in select)
    if not normalized or any(not value for value in normalized):
        raise ValueError("select must include at least one non-empty column name.")
    return normalized


def _selects_all_columns(select: str | list[str] | tuple[str, ...]) -> bool:
    return isinstance(select, str) and select.strip() == "*"


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


def _materialize_frame(df: FrameLike) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df
    if isinstance(df, pl.LazyFrame):
        return df.collect()
    raise TypeError("df must be a Polars DataFrame or LazyFrame.")


def _resolved_db_path(db_path: str | Path) -> Path:
    resolved = Path(db_path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved

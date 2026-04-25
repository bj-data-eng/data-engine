"""Public one-shot DuckDB helpers for flow authoring."""

from __future__ import annotations

import duckdb as duckdb

from data_engine.helpers.duckdb._dimensions import attach_dimension
from data_engine.helpers.duckdb._dimensions import build_dimension
from data_engine.helpers.duckdb._dimensions import denormalize_columns
from data_engine.helpers.duckdb._dimensions import normalize_columns
from data_engine.helpers.duckdb._maintenance import compact_database
from data_engine.helpers.duckdb._maintenance import ensure_index
from data_engine.helpers.duckdb._read import read_rows_by_values
from data_engine.helpers.duckdb._read import read_sql
from data_engine.helpers.duckdb._read import read_table
from data_engine.helpers.duckdb._replace import replace_rows_by_file
from data_engine.helpers.duckdb._replace import replace_rows_by_values
from data_engine.helpers.duckdb._replace import replace_table

__all__ = [
    "attach_dimension",
    "build_dimension",
    "compact_database",
    "denormalize_columns",
    "ensure_index",
    "normalize_columns",
    "read_rows_by_values",
    "read_sql",
    "read_table",
    "replace_rows_by_file",
    "replace_rows_by_values",
    "replace_table",
]

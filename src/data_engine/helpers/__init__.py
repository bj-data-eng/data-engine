"""Public authoring helper modules."""

from data_engine.helpers.duckdb import attach_dimension
from data_engine.helpers.duckdb import build_dimension
from data_engine.helpers.duckdb import denormalize_columns
from data_engine.helpers.duckdb import normalize_columns
from data_engine.helpers.duckdb import read_rows_by_values
from data_engine.helpers.duckdb import read_sql
from data_engine.helpers.duckdb import read_table
from data_engine.helpers.duckdb import replace_rows_by_file
from data_engine.helpers.duckdb import replace_rows_by_values
from data_engine.helpers.duckdb import replace_table
from data_engine.helpers.polars import DataEngineDataFrameNamespace
from data_engine.helpers.polars import DataEngineLazyFrameNamespace
from data_engine.helpers.polars import sink_parquet_atomic
from data_engine.helpers.polars import write_excel_atomic
from data_engine.helpers.polars import write_parquet_atomic
from data_engine.helpers.schema import TableSchema
from data_engine.helpers.schema import normalize_column_name
from data_engine.helpers.schema import normalize_column_names
from data_engine.helpers.schema import normalized_column_renames

__all__ = [
    "TableSchema",
    "DataEngineDataFrameNamespace",
    "DataEngineLazyFrameNamespace",
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
    "sink_parquet_atomic",
    "write_excel_atomic",
    "write_parquet_atomic",
    "normalize_column_name",
    "normalize_column_names",
    "normalized_column_renames",
]

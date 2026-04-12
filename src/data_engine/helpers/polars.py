"""Polars namespace helpers for Data Engine flow authoring."""

from __future__ import annotations

from collections.abc import Callable, Iterable
import os
from pathlib import Path
from uuid import uuid4

import polars as pl

from data_engine.helpers.duckdb import attach_dimension as _attach_dimension
from data_engine.helpers.duckdb import build_dimension as _build_dimension
from data_engine.helpers.duckdb import denormalize_columns as _denormalize_columns
from data_engine.helpers.duckdb import normalize_columns as _normalize_columns
from data_engine.helpers.duckdb import replace_rows_by_file as _replace_rows_by_file
from data_engine.helpers.duckdb import replace_rows_by_values as _replace_rows_by_values
from data_engine.helpers.duckdb import replace_table as _replace_table
from data_engine.helpers.schema import normalize_column_names as _normalize_column_names

PathLike = str | os.PathLike[str]
ColumnNames = str | list[str] | tuple[str, ...]
ReturnMode = str | None


def write_parquet_atomic(df: pl.DataFrame, path: PathLike, **write_options: object) -> Path:
    """Write a Polars dataframe to parquet with same-directory atomic replacement.

    The dataframe is first written to a unique temporary file beside the target,
    then moved into place with ``os.replace``. This keeps readers from seeing a
    partially written parquet file while preserving normal Polars write options.

    Parameters
    ----------
    df : pl.DataFrame
        Eager Polars dataframe to write.
    path : PathLike
        Target parquet path.
    **write_options : object
        Keyword options forwarded to ``pl.DataFrame.write_parquet``.

    Returns
    -------
    Path
        Absolute target path that was replaced.

    Examples
    --------
    .. code-block:: python

        import polars as pl

        from data_engine.helpers import write_parquet_atomic

        target = write_parquet_atomic(
            pl.DataFrame({"claim_id": [1, 2]}),
            "workspaces/example/output/claims.parquet",
        )

        df = pl.DataFrame({"claim_id": [3]})
        df.de.write_parquet_atomic(target)
    """
    return _write_atomic(path, lambda temporary_path: df.write_parquet(temporary_path, **write_options))


def write_excel_atomic(
    df: pl.DataFrame,
    path: PathLike,
    worksheet: str | None = None,
    **write_options: object,
) -> Path:
    """Write a Polars dataframe to Excel with same-directory atomic replacement.

    The dataframe is first written to a unique temporary workbook beside the
    target, then moved into place with ``os.replace``. All keyword options are
    forwarded to ``pl.DataFrame.write_excel``.

    Parameters
    ----------
    df : pl.DataFrame
        Eager Polars dataframe to write.
    path : PathLike
        Target Excel workbook path.
    worksheet : str | None
        Optional worksheet name forwarded to ``pl.DataFrame.write_excel``.
    **write_options : object
        Keyword options forwarded to ``pl.DataFrame.write_excel``.

    Returns
    -------
    Path
        Absolute target path that was replaced.

    Examples
    --------
    .. code-block:: python

        import polars as pl

        from data_engine.helpers import write_excel_atomic

        target = write_excel_atomic(
            pl.DataFrame({"claim_id": [1, 2]}),
            "workspaces/example/output/claims.xlsx",
            worksheet="Claims",
            table_name="claims",
            autofit=True,
        )

        df = pl.DataFrame({"claim_id": [3]})
        df.de.write_excel_atomic(target, worksheet="Claims")
    """
    return _write_atomic(
        path,
        lambda temporary_path: df.write_excel(temporary_path, worksheet=worksheet, **write_options),
    )


def sink_parquet_atomic(lf: pl.LazyFrame, path: PathLike, **sink_options: object) -> Path:
    """Sink a Polars lazy frame to parquet with same-directory atomic replacement.

    The lazy query is executed into a unique temporary file beside the target,
    then moved into place with ``os.replace``. Use the default eager sink mode so
    the helper can complete the replacement in the same call.

    Parameters
    ----------
    lf : pl.LazyFrame
        Lazy Polars frame to execute and write.
    path : PathLike
        Target parquet path.
    **sink_options : object
        Keyword options forwarded to ``pl.LazyFrame.sink_parquet``.

    Returns
    -------
    Path
        Absolute target path that was replaced.

    Raises
    ------
    ValueError
        If ``lazy=True`` is passed.

    Examples
    --------
    .. code-block:: python

        import polars as pl

        import data_engine.helpers

        lf = pl.DataFrame({"claim_id": [1, 2]}).lazy()
        lf.de.sink_parquet_atomic("workspaces/example/output/claims.parquet")
    """
    if sink_options.get("lazy") is True:
        raise ValueError("Atomic LazyFrame parquet writes require eager sink execution; pass lazy=False or omit lazy.")
    return _write_atomic(path, lambda temporary_path: lf.sink_parquet(temporary_path, **sink_options))


def _write_atomic(path: PathLike, write: Callable[[Path], object]) -> Path:
    target_path = Path(path).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = target_path.with_name(f".{target_path.name}.{uuid4().hex}.tmp")
    try:
        write(temporary_path)
        os.replace(temporary_path, target_path)
    except BaseException:
        _remove_temporary_file(temporary_path)
        raise
    return target_path


def _remove_temporary_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


@pl.api.register_dataframe_namespace("de")
class DataEngineDataFrameNamespace:
    """Data Engine helpers available from ``pl.DataFrame.de``."""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def normalize_column_names(self, columns: Iterable[object] | None = None) -> pl.DataFrame:
        """Normalize column names on this dataframe.

        Parameters
        ----------
        columns : Iterable[object] | None
            Optional subset of column names to normalize. When omitted, all
            dataframe columns are normalized.

        Returns
        -------
        pl.DataFrame
            Dataframe with normalized column names.
        """
        return _normalize_column_names(self._df, columns)

    def write_parquet_atomic(self, path: PathLike, **write_options: object) -> Path:
        """Write this dataframe to parquet with atomic target replacement.

        Parameters
        ----------
        path : PathLike
            Target parquet path.
        **write_options : object
            Keyword options forwarded to ``pl.DataFrame.write_parquet``.

        Returns
        -------
        Path
            Absolute target path that was replaced.
        """
        return write_parquet_atomic(self._df, path, **write_options)

    def write_excel_atomic(
        self,
        path: PathLike,
        worksheet: str | None = None,
        **write_options: object,
    ) -> Path:
        """Write this dataframe to Excel with atomic target replacement.

        Parameters
        ----------
        path : PathLike
            Target Excel workbook path.
        worksheet : str | None
            Optional worksheet name forwarded to ``pl.DataFrame.write_excel``.
        **write_options : object
            Keyword options forwarded to ``pl.DataFrame.write_excel``.

        Returns
        -------
        Path
            Absolute target path that was replaced.
        """
        return write_excel_atomic(self._df, path, worksheet=worksheet, **write_options)

    def build_dimension(
        self,
        db_path: PathLike,
        table: str,
        *,
        key_column: str = "dimension_key",
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Build or extend one DuckDB dimension table from this dataframe.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        key_column : str
            Surrogate key column to create in the dimension table.
        return_df : bool
            Whether to return the mapping dataframe for this dataframe's
            natural keys.

        Returns
        -------
        pl.DataFrame | None
            Mapping dataframe when ``return_df`` is true; otherwise ``None``.
        """
        return _build_dimension(db_path, table, df=self._df, key_column=key_column, return_df=return_df)

    def attach_dimension(
        self,
        db_path: PathLike,
        table: str,
        *,
        on: ColumnNames,
        key_column: str = "dimension_key",
        drop_key: bool = False,
    ) -> pl.DataFrame:
        """Attach an existing DuckDB dimension key to this dataframe.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        on : ColumnNames
            Natural key column or columns used to join to the dimension table.
        key_column : str
            Surrogate key column to attach.
        drop_key : bool
            Whether to drop the natural key columns after attaching the
            surrogate key.

        Returns
        -------
        pl.DataFrame
            Dataframe with the surrogate key column attached.
        """
        return _attach_dimension(
            db_path,
            table,
            df=self._df,
            on=on,
            key_column=key_column,
            drop_key=drop_key,
        )

    def denormalize_columns(
        self,
        db_path: PathLike,
        table: str,
        *,
        key_column: str = "dimension_key",
        select: ColumnNames = "*",
        drop_key: bool = False,
    ) -> pl.DataFrame:
        """Attach natural columns from an existing DuckDB dimension table.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        key_column : str
            Surrogate key column used to join to the dimension table.
        select : ColumnNames
            Natural columns to attach, or ``"*"`` for all non-key columns.
        drop_key : bool
            Whether to drop ``key_column`` after attaching the natural columns.

        Returns
        -------
        pl.DataFrame
            Dataframe with selected dimension columns attached.
        """
        return _denormalize_columns(
            db_path,
            table,
            df=self._df,
            key_column=key_column,
            select=select,
            drop_key=drop_key,
        )

    def normalize_columns(
        self,
        db_path: PathLike,
        table: str,
        *,
        on: ColumnNames,
        key_column: str = "dimension_key",
        drop_key: bool = True,
        returns: ReturnMode = "df",
    ) -> pl.DataFrame | None:
        """Build dimension keys and attach them back onto this dataframe.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        on : ColumnNames
            Natural key column or columns used to build the dimension.
        key_column : str
            Surrogate key column to create and attach.
        drop_key : bool
            Whether to drop natural key columns after attaching the surrogate
            key.
        returns : ReturnMode
            ``"df"`` for normalized input rows, ``"map"`` for only the key
            mapping, or ``None`` to only persist dimension rows.

        Returns
        -------
        pl.DataFrame | None
            Normalized dataframe, mapping dataframe, or ``None`` according to
            ``returns``.
        """
        return _normalize_columns(
            db_path,
            table,
            df=self._df,
            on=on,
            key_column=key_column,
            drop_key=drop_key,
            returns=returns,
        )

    def replace_rows_by_file(
        self,
        db_path: PathLike,
        table: str,
        *,
        file_hash: str,
        file_hash_column: str = "file_key",
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Replace one file's DuckDB rows and append this dataframe.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Destination table name, optionally schema-qualified.
        file_hash : str
            Stable source-file identifier used to delete the previous batch.
        file_hash_column : str
            Column name used to store ``file_hash`` in the destination table.
        return_df : bool
            Whether to return this dataframe with the file hash column attached.

        Returns
        -------
        pl.DataFrame | None
            Inserted rows with ``file_hash_column`` when ``return_df`` is true;
            otherwise ``None``.
        """
        return _replace_rows_by_file(
            db_path,
            table,
            df=self._df,
            file_hash=file_hash,
            file_hash_column=file_hash_column,
            return_df=return_df,
        )

    def replace_rows_by_values(
        self,
        db_path: PathLike,
        table: str,
        *,
        column: str,
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Replace DuckDB rows matching this dataframe's values for one column.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Destination table name, optionally schema-qualified.
        column : str
            Column whose incoming values define the rows to replace.
        return_df : bool
            Whether to return the inserted dataframe.

        Returns
        -------
        pl.DataFrame | None
            Inserted dataframe when ``return_df`` is true; otherwise ``None``.
        """
        return _replace_rows_by_values(db_path, table, df=self._df, column=column, return_df=return_df)

    def replace_table(
        self,
        db_path: PathLike,
        table: str,
        *,
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Replace one DuckDB table wholesale from this dataframe.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Destination table name, optionally schema-qualified.
        return_df : bool
            Whether to return the inserted dataframe.

        Returns
        -------
        pl.DataFrame | None
            Inserted dataframe when ``return_df`` is true; otherwise ``None``.
        """
        return _replace_table(db_path, table, df=self._df, return_df=return_df)


@pl.api.register_lazyframe_namespace("de")
class DataEngineLazyFrameNamespace:
    """Data Engine helpers available from ``pl.LazyFrame.de``."""

    def __init__(self, lf: pl.LazyFrame) -> None:
        self._lf = lf

    def normalize_column_names(self, columns: Iterable[object] | None = None) -> pl.LazyFrame:
        """Normalize column names on this lazy frame.

        Parameters
        ----------
        columns : Iterable[object] | None
            Optional subset of column names to normalize. When omitted, all
            lazy-frame columns are normalized.

        Returns
        -------
        pl.LazyFrame
            Lazy frame with normalized column names.
        """
        return _normalize_column_names(self._lf, columns)

    def sink_parquet_atomic(self, path: PathLike, **sink_options: object) -> Path:
        """Execute this lazy frame to parquet with atomic target replacement.

        Parameters
        ----------
        path : PathLike
            Target parquet path.
        **sink_options : object
            Keyword options forwarded to ``pl.LazyFrame.sink_parquet``.

        Returns
        -------
        Path
            Absolute target path that was replaced.
        """
        return sink_parquet_atomic(self._lf, path, **sink_options)

    def build_dimension(
        self,
        db_path: PathLike,
        table: str,
        *,
        key_column: str = "dimension_key",
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Build or extend one DuckDB dimension table from this lazy frame.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        key_column : str
            Surrogate key column to create in the dimension table.
        return_df : bool
            Whether to return the mapping dataframe for this lazy frame's
            natural keys.

        Returns
        -------
        pl.DataFrame | None
            Mapping dataframe when ``return_df`` is true; otherwise ``None``.
        """
        return _build_dimension(db_path, table, df=self._lf, key_column=key_column, return_df=return_df)

    def attach_dimension(
        self,
        db_path: PathLike,
        table: str,
        *,
        on: ColumnNames,
        key_column: str = "dimension_key",
        drop_key: bool = False,
    ) -> pl.DataFrame:
        """Attach an existing DuckDB dimension key to this lazy frame.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        on : ColumnNames
            Natural key column or columns used to join to the dimension table.
        key_column : str
            Surrogate key column to attach.
        drop_key : bool
            Whether to drop the natural key columns after attaching the
            surrogate key.

        Returns
        -------
        pl.DataFrame
            Collected dataframe with the surrogate key column attached.
        """
        return _attach_dimension(
            db_path,
            table,
            df=self._lf,
            on=on,
            key_column=key_column,
            drop_key=drop_key,
        )

    def denormalize_columns(
        self,
        db_path: PathLike,
        table: str,
        *,
        key_column: str = "dimension_key",
        select: ColumnNames = "*",
        drop_key: bool = False,
    ) -> pl.DataFrame:
        """Attach natural columns from an existing DuckDB dimension table.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        key_column : str
            Surrogate key column used to join to the dimension table.
        select : ColumnNames
            Natural columns to attach, or ``"*"`` for all non-key columns.
        drop_key : bool
            Whether to drop ``key_column`` after attaching the natural columns.

        Returns
        -------
        pl.DataFrame
            Collected dataframe with selected dimension columns attached.
        """
        return _denormalize_columns(
            db_path,
            table,
            df=self._lf,
            key_column=key_column,
            select=select,
            drop_key=drop_key,
        )

    def normalize_columns(
        self,
        db_path: PathLike,
        table: str,
        *,
        on: ColumnNames,
        key_column: str = "dimension_key",
        drop_key: bool = True,
        returns: ReturnMode = "df",
    ) -> pl.DataFrame | None:
        """Build dimension keys and attach them back onto this lazy frame.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Dimension table name, optionally schema-qualified.
        on : ColumnNames
            Natural key column or columns used to build the dimension.
        key_column : str
            Surrogate key column to create and attach.
        drop_key : bool
            Whether to drop natural key columns after attaching the surrogate
            key.
        returns : ReturnMode
            ``"df"`` for normalized input rows, ``"map"`` for only the key
            mapping, or ``None`` to only persist dimension rows.

        Returns
        -------
        pl.DataFrame | None
            Normalized dataframe, mapping dataframe, or ``None`` according to
            ``returns``.
        """
        return _normalize_columns(
            db_path,
            table,
            df=self._lf,
            on=on,
            key_column=key_column,
            drop_key=drop_key,
            returns=returns,
        )

    def replace_rows_by_file(
        self,
        db_path: PathLike,
        table: str,
        *,
        file_hash: str,
        file_hash_column: str = "file_key",
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Replace one file's DuckDB rows and append this lazy frame.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Destination table name, optionally schema-qualified.
        file_hash : str
            Stable source-file identifier used to delete the previous batch.
        file_hash_column : str
            Column name used to store ``file_hash`` in the destination table.
        return_df : bool
            Whether to return the collected frame with the file hash column
            attached.

        Returns
        -------
        pl.DataFrame | None
            Inserted rows with ``file_hash_column`` when ``return_df`` is true;
            otherwise ``None``.
        """
        return _replace_rows_by_file(
            db_path,
            table,
            df=self._lf,
            file_hash=file_hash,
            file_hash_column=file_hash_column,
            return_df=return_df,
        )

    def replace_rows_by_values(
        self,
        db_path: PathLike,
        table: str,
        *,
        column: str,
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Replace DuckDB rows matching this lazy frame's values for one column.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Destination table name, optionally schema-qualified.
        column : str
            Column whose incoming values define the rows to replace.
        return_df : bool
            Whether to return the inserted dataframe.

        Returns
        -------
        pl.DataFrame | None
            Inserted dataframe when ``return_df`` is true; otherwise ``None``.
        """
        return _replace_rows_by_values(db_path, table, df=self._lf, column=column, return_df=return_df)

    def replace_table(
        self,
        db_path: PathLike,
        table: str,
        *,
        return_df: bool = True,
    ) -> pl.DataFrame | None:
        """Replace one DuckDB table wholesale from this lazy frame.

        Parameters
        ----------
        db_path : PathLike
            DuckDB database file path.
        table : str
            Destination table name, optionally schema-qualified.
        return_df : bool
            Whether to return the inserted dataframe.

        Returns
        -------
        pl.DataFrame | None
            Inserted dataframe when ``return_df`` is true; otherwise ``None``.
        """
        return _replace_table(db_path, table, df=self._lf, return_df=return_df)

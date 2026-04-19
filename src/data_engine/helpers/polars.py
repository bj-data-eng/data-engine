"""Polars namespace helpers for Data Engine flow authoring."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date, datetime
import os
from pathlib import Path
import time
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
WeekMask = tuple[bool, bool, bool, bool, bool, bool, bool]
DateLike = date | datetime
ExprLike = pl.Expr | str | DateLike
IntExprLike = pl.Expr | str | int
_DEFAULT_WEEK_MASK: WeekMask = (True, True, True, True, True, False, False)


def networkdays(
    start: ExprLike,
    end: ExprLike,
    *,
    holidays: list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None = None,
    count_first_day: bool = False,
    mask: Iterable[bool] | None = None,
) -> pl.Expr:
    """Return Excel-style business-day counts as a Polars expression.

    This helper matches Excel ``NETWORKDAYS`` semantics by counting both
    endpoints when they are business days. Weekends default to Saturday/Sunday,
    and optional holidays are excluded from the count.

    The one intentional extension is ``count_first_day``. When enabled, the
    start date is still counted even if it falls on a masked weekday or one of
    the supplied holidays.

    Parameters
    ----------
    start : pl.Expr | str | date | datetime
        Start date expression, column name, or scalar date/datetime.
    end : pl.Expr | str | date | datetime
        End date expression, column name, or scalar date/datetime.
    holidays : list[date | datetime | str] | tuple[...] | set[...] | None
        Optional holiday dates removed from the business-day count. String
        values must use ISO date text such as ``"2026-04-15"``.
    count_first_day : bool
        Whether to force the first day into the count when it would normally be
        excluded by the weekday mask or holiday list.
    mask : Iterable[bool] | None
        Monday-first seven-item business-day mask. Every item must be a real
        ``bool``. ``None`` uses the Excel default: Monday-Friday counted,
        Saturday-Sunday excluded.

    Returns
    -------
    pl.Expr
        Expression that evaluates to the signed business-day count. Datetime
        inputs are normalized to their calendar date before counting.

    Examples
    --------
    Add a row-level business-day count:

    .. code-block:: python

        from datetime import date
        import polars as pl

        import data_engine.helpers

        df = pl.DataFrame(
            {
                "received_date": [date(2026, 4, 13), date(2026, 4, 14)],
                "due_date": [date(2026, 4, 17), date(2026, 4, 21)],
            }
        ).with_columns(
            business_days=data_engine.helpers.networkdays(
                "received_date",
                "due_date",
                holidays=[date(2026, 4, 15)],
            )
        )

    Use scalar datetimes and count the first day:

    .. code-block:: python

        from datetime import datetime

        df = df.with_columns(
            sla_days=data_engine.helpers.networkdays(
                datetime(2026, 4, 13, 8, 30),
                pl.col("resolved_at"),
                count_first_day=True,
            )
        )

    Chain the expression into a grouped cumulative total:

    .. code-block:: python

        df = (
            df.sort(["claim_id", "sequence_number"])
            .with_columns(
                cumulative_business_days=
                pl.when(pl.col("use_days"))
                .then(
                    data_engine.helpers.networkdays(
                        "start_date",
                        "end_date",
                        holidays=[date(2026, 4, 15)],
                    )
                )
                .otherwise(pl.lit(0))
                .cum_sum()
                .over("claim_id")
            )
        )

    Notes
    -----
    ``networkdays(...)`` returns a normal ``pl.Expr``. You can chain it into
    ``cum_sum()``, window expressions, filters, and any other Polars expression
    pipeline.
    """
    week_mask = _coerce_week_mask(mask)
    holiday_dates = _coerce_holiday_dates(holidays)
    start_expr = _as_date_expr(start)
    end_expr = _as_date_expr(end)
    forward_expr = pl.business_day_count(
        start_expr,
        end_expr + pl.duration(days=1),
        week_mask=week_mask,
        holidays=holiday_dates,
    )
    backward_expr = -pl.business_day_count(
        end_expr,
        start_expr + pl.duration(days=1),
        week_mask=week_mask,
        holidays=holiday_dates,
    )
    result = pl.when(start_expr <= end_expr).then(forward_expr).otherwise(backward_expr)
    if count_first_day:
        result = result + _forced_first_day_adjustment(start_expr, end_expr, week_mask, holiday_dates)
    return pl.when(start_expr.is_null() | end_expr.is_null()).then(pl.lit(None, dtype=pl.Int64)).otherwise(result)


def workday(
    start: ExprLike,
    days: IntExprLike,
    *,
    holidays: list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None = None,
    count_first_day: bool = False,
    mask: Iterable[bool] | None = None,
) -> pl.Expr:
    """Return Excel-style workday offsets as a Polars expression.

    This helper mirrors Excel ``WORKDAY`` by returning the business date that
    falls the requested number of working days before or after ``start``.

    The one intentional extension is ``count_first_day``. When enabled, the
    start date itself can be day 1, even if it falls on a masked weekday or one
    of the supplied holidays.

    Parameters
    ----------
    start : pl.Expr | str | date | datetime
        Start date expression, column name, or scalar date/datetime.
    days : pl.Expr | str | int
        Signed business-day offset expression, column name, or scalar integer.
    holidays : list[date | datetime | str] | tuple[...] | set[...] | None
        Optional holiday dates skipped while calculating the result. String
        values must use ISO date text such as ``"2026-04-15"``.
    count_first_day : bool
        Whether the start date itself can count as day 1 when moving forward or
        backward through business days.
    mask : Iterable[bool] | None
        Monday-first seven-item business-day mask. Every item must be a real
        ``bool``. ``None`` uses the Excel default: Monday-Friday counted,
        Saturday-Sunday excluded.

    Returns
    -------
    pl.Expr
        Expression that evaluates to a ``Date`` result. Datetime inputs are
        normalized to their calendar date before offsetting.

    Examples
    --------
    Add one target business date column:

    .. code-block:: python

        from datetime import date
        import polars as pl

        import data_engine.helpers

        df = pl.DataFrame(
            {
                "received_date": [date(2026, 4, 13), date(2026, 4, 14)],
                "sla_days": [3, 5],
            }
        ).with_columns(
            due_date=data_engine.helpers.workday(
                "received_date",
                "sla_days",
                holidays=[date(2026, 4, 15)],
            )
        )

    Count the start date as day 1:

    .. code-block:: python

        df = df.with_columns(
            due_date=data_engine.helpers.workday(
                "received_date",
                "sla_days",
                holidays=[date(2026, 4, 15)],
                count_first_day=True,
            )
        )

    Use a custom weekday mask where Saturday is also a business day:

    .. code-block:: python

        df = df.with_columns(
            due_date=data_engine.helpers.workday(
                "received_date",
                "sla_days",
                mask=(True, True, True, True, True, True, False),
            )
        )
    """
    week_mask = _coerce_week_mask(mask)
    holiday_dates = _coerce_holiday_dates(holidays)
    start_expr = _as_date_expr(start)
    days_expr = _as_int_expr(days).cast(pl.Int64)
    is_business = _is_business_day_expr(start_expr, week_mask, holiday_dates)
    default_result = _workday_result(
        start_expr,
        days_expr,
        week_mask,
        holiday_dates,
        count_first_day=False,
        is_business=is_business,
    )
    counted_result = _workday_result(
        start_expr,
        days_expr,
        week_mask,
        holiday_dates,
        count_first_day=True,
        is_business=is_business,
    )
    result = counted_result if count_first_day else default_result
    return pl.when(start_expr.is_null() | days_expr.is_null()).then(pl.lit(None, dtype=pl.Date)).otherwise(result)


def _coerce_week_mask(mask: Iterable[bool] | None) -> WeekMask:
    if mask is None:
        return _DEFAULT_WEEK_MASK
    values = tuple(mask)
    if len(values) != 7:
        raise ValueError("mask must contain exactly seven Monday-first boolean values.")
    if not all(isinstance(value, bool) for value in values):
        raise TypeError("mask must contain exactly seven Monday-first boolean values.")
    return values  # type: ignore[return-value]


def _coerce_holiday_dates(
    holidays: list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None,
) -> tuple[date, ...]:
    if holidays is None:
        return ()
    values: set[date] = set()
    for value in holidays:
        if isinstance(value, datetime):
            values.add(value.date())
            continue
        if isinstance(value, date):
            values.add(value)
            continue
        if isinstance(value, str):
            values.add(date.fromisoformat(value))
            continue
        raise TypeError("holidays must contain date, datetime, or ISO date string values.")
    return tuple(sorted(values))


def _as_date_expr(value: ExprLike) -> pl.Expr:
    if isinstance(value, pl.Expr):
        return value.cast(pl.Date)
    if isinstance(value, str):
        return pl.col(value).cast(pl.Date)
    return pl.lit(value).cast(pl.Date)


def _as_int_expr(value: IntExprLike) -> pl.Expr:
    if isinstance(value, pl.Expr):
        return value
    if isinstance(value, str):
        return pl.col(value)
    return pl.lit(value)


def _is_business_day_expr(
    date_expr: pl.Expr,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
) -> pl.Expr:
    weekday = date_expr.dt.weekday()
    day_allowed = pl.lit(False)
    for index, allowed in enumerate(week_mask, start=1):
        day_allowed = pl.when(weekday == index).then(pl.lit(allowed)).otherwise(day_allowed)
    holiday_expr = (
        date_expr.is_in(pl.lit(list(holiday_dates), dtype=pl.List(pl.Date)))
        if holiday_dates
        else pl.lit(False)
    )
    return day_allowed & ~holiday_expr


def _forced_first_day_adjustment(
    start_expr: pl.Expr,
    end_expr: pl.Expr,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
) -> pl.Expr:
    start_already_counted = _is_business_day_expr(start_expr, week_mask, holiday_dates)
    return (
        pl.when(~start_already_counted)
        .then(pl.when(start_expr <= end_expr).then(pl.lit(1)).otherwise(pl.lit(-1)))
        .otherwise(pl.lit(0))
    )


def _workday_result(
    start_expr: pl.Expr,
    days_expr: pl.Expr,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
    *,
    count_first_day: bool,
    is_business: pl.Expr,
) -> pl.Expr:
    kwargs = {"week_mask": week_mask, "holidays": holiday_dates}
    if count_first_day:
        business_result = (
            pl.when(days_expr > 0)
            .then(start_expr.dt.add_business_days(days_expr - 1, roll="forward", **kwargs))
            .when(days_expr < 0)
            .then(start_expr.dt.add_business_days(days_expr + 1, roll="backward", **kwargs))
            .otherwise(start_expr)
        )
        nonbusiness_result = (
            pl.when(days_expr > 0)
            .then(
                pl.when(days_expr == 1)
                .then(start_expr)
                .otherwise(start_expr.dt.add_business_days(days_expr - 2, roll="forward", **kwargs))
            )
            .when(days_expr < 0)
            .then(
                pl.when(days_expr == -1)
                .then(start_expr)
                .otherwise(start_expr.dt.add_business_days(days_expr + 2, roll="backward", **kwargs))
            )
            .otherwise(start_expr)
        )
    else:
        business_result = (
            pl.when(days_expr >= 0)
            .then(start_expr.dt.add_business_days(days_expr, roll="forward", **kwargs))
            .otherwise(start_expr.dt.add_business_days(days_expr, roll="backward", **kwargs))
        )
        nonbusiness_result = (
            pl.when(days_expr > 0)
            .then(start_expr.dt.add_business_days(days_expr - 1, roll="forward", **kwargs))
            .when(days_expr < 0)
            .then(start_expr.dt.add_business_days(days_expr + 1, roll="backward", **kwargs))
            .otherwise(start_expr.dt.add_business_days(pl.lit(0), roll="forward", **kwargs))
        )
    return pl.when(is_business).then(business_result).otherwise(nonbusiness_result)


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
        _replace_atomic(temporary_path, target_path)
    except BaseException:
        _remove_temporary_file(temporary_path)
        raise
    return target_path


def _replace_atomic(source_path: Path, target_path: Path) -> None:
    backoff_seconds = (0.0, 0.02, 0.05, 0.1, 0.2)
    last_error: BaseException | None = None
    for delay_seconds in backoff_seconds:
        if delay_seconds > 0.0:
            time.sleep(delay_seconds)
        try:
            os.replace(source_path, target_path)
            return
        except PermissionError as exc:
            if os.name != "nt" or getattr(exc, "winerror", None) != 5:
                raise
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    os.replace(source_path, target_path)


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

    def networkdays(
        self,
        start: ExprLike,
        end: ExprLike,
        *,
        holidays: list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style business-day count expression for this dataframe.

        This is a convenience wrapper around :func:`data_engine.helpers.networkdays`.
        The returned value is still a normal ``pl.Expr``, so it can be chained
        into cumulative windows and other Polars expressions.

        Example
        -------
        .. code-block:: python

            df = df.with_columns(
                business_days=df.de.networkdays(
                    "start_date",
                    "end_date",
                    holidays=[date(2026, 4, 15)],
                )
            )

            df = df.sort(["claim_id", "sequence_number"]).with_columns(
                cumulative_business_days=
                pl.when(pl.col("use_days"))
                .then(df.de.networkdays("start_date", "end_date"))
                .otherwise(pl.lit(0))
                .cum_sum()
                .over("claim_id")
            )
        """
        return networkdays(
            start,
            end,
            holidays=holidays,
            count_first_day=count_first_day,
            mask=mask,
        )

    def workday(
        self,
        start: ExprLike,
        days: IntExprLike,
        *,
        holidays: list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style workday offset expression for this dataframe.

        This is a convenience wrapper around :func:`data_engine.helpers.workday`.

        Example
        -------
        .. code-block:: python

            df = df.with_columns(
                due_date=df.de.workday(
                    "received_date",
                    "sla_days",
                    holidays=[date(2026, 4, 15)],
                )
            )
        """
        return workday(
            start,
            days,
            holidays=holidays,
            count_first_day=count_first_day,
            mask=mask,
        )

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

    def networkdays(
        self,
        start: ExprLike,
        end: ExprLike,
        *,
        holidays: list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style business-day count expression for this lazy frame.

        This is a convenience wrapper around :func:`data_engine.helpers.networkdays`.
        The returned value stays lazy and can be chained into window
        expressions before ``collect()``.
        """
        return networkdays(
            start,
            end,
            holidays=holidays,
            count_first_day=count_first_day,
            mask=mask,
        )

    def workday(
        self,
        start: ExprLike,
        days: IntExprLike,
        *,
        holidays: list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style workday offset expression for this lazy frame.

        This is a convenience wrapper around :func:`data_engine.helpers.workday`.
        """
        return workday(
            start,
            days,
            holidays=holidays,
            count_first_day=count_first_day,
            mask=mask,
        )

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

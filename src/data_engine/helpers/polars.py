"""Polars namespace helpers for Data Engine flow authoring."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime, timedelta
import os
from pathlib import Path
import time
from uuid import uuid4

import numpy as np
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
HolidayDates = list[DateLike | str] | tuple[DateLike | str, ...] | set[DateLike | str] | None
ExprLike = pl.Expr | str | DateLike
IntExprLike = pl.Expr | str | int
ColumnExpr = str | pl.Expr
ColumnExprs = ColumnExpr | Sequence[ColumnExpr]
DescendingLike = bool | Sequence[bool]
PolarsFrame = pl.DataFrame | pl.LazyFrame
_DEFAULT_WEEK_MASK: WeekMask = (True, True, True, True, True, False, False)


def networkdays(
    start: ExprLike,
    end: ExprLike,
    *,
    holidays: HolidayDates = None,
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
    start : ExprLike
        Start date expression, column name, or scalar date/datetime.
    end : ExprLike
        End date expression, column name, or scalar date/datetime.
    holidays : HolidayDates
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
    return pl.struct(
        start=start_expr,
        end=end_expr,
    ).map_batches(
        lambda batch: _networkdays_batch(
            batch.struct.field("start"),
            batch.struct.field("end"),
            week_mask=week_mask,
            holiday_dates=holiday_dates,
            count_first_day=count_first_day,
        ),
        return_dtype=pl.Int64,
    )


def workday(
    start: ExprLike,
    days: IntExprLike,
    *,
    holidays: HolidayDates = None,
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
    start : ExprLike
        Start date expression, column name, or scalar date/datetime.
    days : IntExprLike
        Signed business-day offset expression, column name, or scalar integer.
    holidays : HolidayDates
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
    return pl.struct(
        start=start_expr,
        days=days_expr,
    ).map_batches(
        lambda batch: _workday_batch(
            batch.struct.field("start"),
            batch.struct.field("days"),
            week_mask=week_mask,
            holiday_dates=holiday_dates,
            count_first_day=count_first_day,
        ),
        return_dtype=pl.Date,
    )


def remove_null_columns(frame: PolarsFrame) -> PolarsFrame:
    """Return a frame without columns that contain no non-null values.

    Columns are kept when at least one row contains a non-null value. Columns
    containing only nulls are removed. For zero-row dataframes, every column is
    considered empty and the returned dataframe has no columns.

    Parameters
    ----------
    frame : PolarsFrame
        Dataframe or lazy frame to trim.

    Returns
    -------
    PolarsFrame
        Frame containing only columns with at least one non-null value. Lazy
        inputs return lazy frames.

    Examples
    --------
    .. code-block:: python

        clean = data_engine.helpers.remove_null_columns(df)

        clean = df.de.remove_null_columns()
    """
    if isinstance(frame, pl.LazyFrame):
        return _remove_null_columns_lazy(frame)
    keep_columns = [name for name in frame.columns if frame.get_column(name).null_count() < frame.height]
    return frame.select(keep_columns)


def _remove_null_columns_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    column_names = tuple(lf.collect_schema().names())
    if not column_names:
        return lf
    counts = lf.select(pl.col(name).is_not_null().sum().alias(name) for name in column_names).collect()
    row = counts.row(0, named=True)
    keep_columns = [name for name in column_names if int(row.get(name) or 0) > 0]
    return lf.select(keep_columns)


def propagate_last_value(
    value: ColumnExpr,
    *,
    by: ColumnExprs,
    sort_by: ColumnExprs,
    where: pl.Expr | None = None,
    descending: DescendingLike = False,
    nulls_last: bool = False,
    ignore_nulls: bool = True,
) -> pl.Expr:
    """Return an expression that broadcasts the last ordered value per window.

    The helper sorts rows inside each ``by`` window, optionally filters the
    ordered rows with ``where``, takes the last ``value`` from that ordered
    candidate set, and propagates it to every row in the same window. Null
    values are ignored by default, which matches the common pattern where only
    one row in a window contains the value to carry across the group.

    Parameters
    ----------
    value : ColumnExpr
        Column name or expression containing the value to propagate.
    by : ColumnExprs
        Window column or columns.
    sort_by : ColumnExprs
        Ordering column or columns used to define the last row in each window.
    where : pl.Expr | None
        Optional row predicate that limits which sorted rows can supply the
        propagated value.
    descending : DescendingLike
        Sort direction passed to ``Expr.sort_by``.
    nulls_last : bool
        Whether null sort-key values are ordered last.
    ignore_nulls : bool
        Whether null ``value`` rows are skipped before taking the last value.

    Returns
    -------
    pl.Expr
        Window expression suitable for ``with_columns`` or ``select``.

    Examples
    --------
    Propagate the latest non-null status to every row for a claim:

    .. code-block:: python

        df = df.with_columns(
            latest_status=data_engine.helpers.propagate_last_value(
                "status",
                by="claim_id",
                sort_by="claim_step_index",
            )
        )

    Propagate the timestamp from the last Archive row to every row for a
    claim. The output column is named by ``with_columns``:

    .. code-block:: python

        df = df.with_columns(
            archived_at=data_engine.helpers.propagate_last_value(
                pl.col("archive_date").dt.combine(pl.col("archive_time")),
                by="claim_id",
                sort_by="claim_step_index",
                where=pl.col("status") == "Archive",
            )
        )

    Compose the predicate to use the last row that is not Archive:

    .. code-block:: python

        df = df.with_columns(
            last_active_at=data_engine.helpers.propagate_last_value(
                pl.col("event_date").dt.combine(pl.col("event_time")),
                by="claim_id",
                sort_by="claim_step_index",
                where=pl.col("status") != "Archive",
            )
        )
    """
    sort_exprs = _as_column_exprs(sort_by)
    value_expr = _as_column_expr(value)
    ordered = value_expr.sort_by(
        sort_exprs,
        descending=descending,
        nulls_last=nulls_last,
    )
    if where is not None:
        ordered = ordered.filter(
            where.sort_by(
                sort_exprs,
                descending=descending,
                nulls_last=nulls_last,
            )
        )
    if ignore_nulls:
        ordered = ordered.drop_nulls()
    return ordered.last().over(_as_column_exprs(by))


def visit_counter(
    value: ColumnExpr,
    *,
    by: ColumnExprs,
    sort_by: ColumnExprs,
    descending: DescendingLike = False,
    nulls_last: bool = False,
) -> pl.Expr:
    """Return a per-value contiguous-run visit number inside each window.

    Rows are ordered inside each ``by`` window, then consecutive rows with the
    same ``value`` are treated as one visit. When a value leaves and later
    returns in the same window, the returned run gets the next visit number for
    that value.

    Parameters
    ----------
    value : ColumnExpr
        Column name or expression containing the state to count visits for.
    by : ColumnExprs
        Window column or columns.
    sort_by : ColumnExprs
        Ordering column or columns used to define row sequence inside each
        window.
    descending : DescendingLike
        Sort direction passed to ``DataFrame.sort`` for the in-window order.
    nulls_last : bool
        Whether null sort-key values are ordered last.

    Returns
    -------
    pl.Expr
        Unsigned integer expression containing the one-based visit number for
        each row's current ``value``.

    Examples
    --------
    Count repeated workflow visits for each document:

    .. code-block:: python

        df = df.with_columns(
            workflow_visit=data_engine.helpers.visit_counter(
                "workflow",
                by="document_id",
                sort_by="step_index",
            )
        )

    For a document with workflow runs ``w1, w1, w1, w2, w2, w1``, the result
    is ``1, 1, 1, 1, 1, 2``.
    """
    sort_exprs = _as_column_exprs(sort_by)
    sort_expr_list = sort_exprs if isinstance(sort_exprs, list) else [sort_exprs]
    sort_names = [f"__sort_{index}" for index in range(len(sort_expr_list))]
    batch_descending: bool | tuple[bool, ...]
    if isinstance(descending, bool):
        batch_descending = descending
    else:
        batch_descending = tuple(descending)

    return (
        pl.struct(
            run_value=_as_column_expr(value),
            **dict(zip(sort_names, sort_expr_list, strict=True)),
        )
        .map_batches(
            lambda batch: _visit_counter_batch(
                batch,
                sort_names=sort_names,
                descending=batch_descending,
                nulls_last=nulls_last,
            ),
            return_dtype=pl.UInt32,
        )
        .over(_as_column_exprs(by))
    )


def _coerce_week_mask(mask: Iterable[bool] | None) -> WeekMask:
    if mask is None:
        return _DEFAULT_WEEK_MASK
    values = tuple(mask)
    if len(values) != 7:
        raise ValueError("mask must contain exactly seven Monday-first boolean values.")
    if not all(isinstance(value, bool) for value in values):
        raise TypeError("mask must contain exactly seven Monday-first boolean values.")
    return values  # type: ignore[return-value]


def _coerce_holiday_dates(holidays: HolidayDates) -> tuple[date, ...]:
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


def _as_column_expr(value: ColumnExpr) -> pl.Expr:
    if isinstance(value, pl.Expr):
        return value
    return pl.col(value)


def _as_column_exprs(value: ColumnExprs) -> pl.Expr | list[pl.Expr]:
    if isinstance(value, str) or isinstance(value, pl.Expr):
        return _as_column_expr(value)
    return [_as_column_expr(item) for item in value]


def _visit_counter_batch(
    batch: pl.Series,
    *,
    sort_names: Sequence[str],
    descending: bool | tuple[bool, ...],
    nulls_last: bool,
) -> pl.Series:
    if len(batch) == 0:
        return pl.Series([], dtype=pl.UInt32)
    frame = batch.struct.unnest().with_row_index("__row_number")
    ordered = frame.sort(
        list(sort_names),
        descending=descending,
        nulls_last=nulls_last,
    )
    visits = [0] * len(frame)
    visit_by_value: dict[object, int] = {}
    previous = object()
    for row in ordered.iter_rows(named=True):
        current = row["run_value"]
        if current != previous:
            visit_by_value[current] = visit_by_value.get(current, 0) + 1
            previous = current
        visits[row["__row_number"]] = visit_by_value[current]
    return pl.Series(visits, dtype=pl.UInt32)


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


def _networkdays_scalar(
    start_date: date | None,
    end_date: date | None,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
    count_first_day: bool,
) -> int | None:
    if start_date is None or end_date is None:
        return None
    if start_date <= end_date:
        result = _forward_networkdays(start_date, end_date, week_mask=week_mask, holiday_dates=holiday_dates)
    else:
        result = -_forward_networkdays(end_date, start_date, week_mask=week_mask, holiday_dates=holiday_dates)
    if count_first_day and not _is_business_day_scalar(start_date, week_mask=week_mask, holiday_dates=holiday_dates):
        return result + (1 if start_date <= end_date else -1)
    return result


def _forward_networkdays(
    start_date: date,
    end_date: date,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
) -> int:
    delta_days = (end_date - start_date).days + 1
    full_weeks, extra_days = divmod(delta_days, 7)
    business_days = (full_weeks + 1) * sum(week_mask)
    for offset in range(1, 8 - extra_days):
        trailing_day = end_date + timedelta(days=offset)
        if week_mask[trailing_day.weekday()]:
            business_days -= 1
    for holiday in holiday_dates:
        if start_date <= holiday <= end_date and week_mask[holiday.weekday()]:
            business_days -= 1
    return business_days


def _is_business_day_scalar(
    value: date,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
) -> bool:
    return week_mask[value.weekday()] and value not in holiday_dates


def _numpy_weekmask_text(week_mask: WeekMask) -> str:
    return "".join("1" if allowed else "0" for allowed in week_mask)


def _numpy_holiday_array(holiday_dates: tuple[date, ...]) -> np.ndarray:
    if not holiday_dates:
        return np.array([], dtype="datetime64[D]")
    return np.array(holiday_dates, dtype="datetime64[D]")


def _numpy_busdaycalendar(week_mask: WeekMask, holiday_dates: tuple[date, ...]) -> np.busdaycalendar:
    return np.busdaycalendar(
        weekmask=_numpy_weekmask_text(week_mask),
        holidays=_numpy_holiday_array(holiday_dates),
    )


def _networkdays_batch(
    start_series: pl.Series,
    end_series: pl.Series,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
    count_first_day: bool,
) -> pl.Series:
    calendar = _numpy_busdaycalendar(week_mask, holiday_dates)
    starts = start_series.to_numpy()
    ends = end_series.to_numpy()
    result = np.full(len(start_series), None, dtype=object)
    valid = ~(np.isnat(starts) | np.isnat(ends))
    if valid.any():
        valid_starts = starts[valid]
        valid_ends = ends[valid]
        forward = valid_starts <= valid_ends
        counts = np.empty(valid_starts.shape[0], dtype=np.int64)
        if forward.any():
            counts[forward] = np.busday_count(
                valid_starts[forward],
                valid_ends[forward] + np.timedelta64(1, "D"),
                busdaycal=calendar,
            )
        if (~forward).any():
            counts[~forward] = -np.busday_count(
                valid_ends[~forward],
                valid_starts[~forward] + np.timedelta64(1, "D"),
                busdaycal=calendar,
            )
        if count_first_day:
            start_business = np.is_busday(valid_starts, busdaycal=calendar)
            counts = counts + np.where(~start_business, np.where(forward, 1, -1), 0)
        result[valid] = counts.tolist()
    return pl.Series(result.tolist(), dtype=pl.Int64)


def _busday_offset_array(
    dates: np.ndarray,
    offsets: np.ndarray,
    *,
    calendar: np.busdaycalendar,
    roll: str,
) -> np.ndarray:
    if dates.size == 0:
        return np.array([], dtype="datetime64[D]")
    return np.busday_offset(dates, offsets, roll=roll, busdaycal=calendar)


def _workday_batch(
    start_series: pl.Series,
    days_series: pl.Series,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
    count_first_day: bool,
) -> pl.Series:
    calendar = _numpy_busdaycalendar(week_mask, holiday_dates)
    starts = start_series.to_numpy()
    days_values = days_series.to_numpy()
    result = np.full(len(start_series), np.datetime64("NaT", "D"), dtype="datetime64[D]")
    valid = ~np.isnat(starts) & ~np.isnan(days_values)
    if valid.any():
        valid_starts = starts[valid]
        valid_days = days_values[valid].astype(np.int64, copy=False)
        valid_result = np.full(valid_starts.shape[0], np.datetime64("NaT", "D"), dtype="datetime64[D]")
        is_business = np.is_busday(valid_starts, busdaycal=calendar)
        next_business = _busday_offset_array(valid_starts, np.zeros(valid_starts.shape[0], dtype=np.int64), calendar=calendar, roll="forward")
        prev_business = _busday_offset_array(valid_starts, np.zeros(valid_starts.shape[0], dtype=np.int64), calendar=calendar, roll="backward")

        zero_mask = valid_days == 0
        pos_mask = valid_days > 0
        neg_mask = valid_days < 0

        if count_first_day:
            valid_result[zero_mask] = valid_starts[zero_mask]

            mask = is_business & pos_mask
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    valid_starts[mask],
                    valid_days[mask] - 1,
                    calendar=calendar,
                    roll="forward",
                )

            mask = is_business & neg_mask
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    valid_starts[mask],
                    valid_days[mask] + 1,
                    calendar=calendar,
                    roll="backward",
                )

            mask = (~is_business) & (valid_days == 1)
            valid_result[mask] = valid_starts[mask]

            mask = (~is_business) & (valid_days > 1)
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    next_business[mask],
                    valid_days[mask] - 2,
                    calendar=calendar,
                    roll="forward",
                )

            mask = (~is_business) & (valid_days == -1)
            valid_result[mask] = valid_starts[mask]

            mask = (~is_business) & (valid_days < -1)
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    prev_business[mask],
                    valid_days[mask] + 2,
                    calendar=calendar,
                    roll="backward",
                )
        else:
            mask = is_business & zero_mask
            valid_result[mask] = valid_starts[mask]

            mask = is_business & pos_mask
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    valid_starts[mask],
                    valid_days[mask],
                    calendar=calendar,
                    roll="forward",
                )

            mask = is_business & neg_mask
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    valid_starts[mask],
                    valid_days[mask],
                    calendar=calendar,
                    roll="backward",
                )

            mask = (~is_business) & zero_mask
            valid_result[mask] = next_business[mask]

            mask = (~is_business) & pos_mask
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    next_business[mask],
                    valid_days[mask] - 1,
                    calendar=calendar,
                    roll="forward",
                )

            mask = (~is_business) & neg_mask
            if mask.any():
                valid_result[mask] = _busday_offset_array(
                    prev_business[mask],
                    valid_days[mask] + 1,
                    calendar=calendar,
                    roll="backward",
                )

        result[valid] = valid_result
    return pl.Series(result.tolist(), dtype=pl.Date)


def _next_business_day(
    value: date,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
) -> date:
    current = value
    while not _is_business_day_scalar(current, week_mask=week_mask, holiday_dates=holiday_dates):
        current += timedelta(days=1)
    return current


def _previous_business_day(
    value: date,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
) -> date:
    current = value
    while not _is_business_day_scalar(current, week_mask=week_mask, holiday_dates=holiday_dates):
        current -= timedelta(days=1)
    return current


def _advance_business_days(
    start_date: date,
    days: int,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
) -> date:
    current = start_date
    remaining = days
    step = 1 if remaining >= 0 else -1
    while remaining != 0:
        current += timedelta(days=step)
        if _is_business_day_scalar(current, week_mask=week_mask, holiday_dates=holiday_dates):
            remaining -= step
    return current


def _workday_scalar(
    start_date: date | None,
    days: int | None,
    *,
    week_mask: WeekMask,
    holiday_dates: tuple[date, ...],
    count_first_day: bool,
) -> date | None:
    if start_date is None or days is None:
        return None
    is_business = _is_business_day_scalar(start_date, week_mask=week_mask, holiday_dates=holiday_dates)
    if count_first_day:
        if days == 0:
            return start_date
        if is_business:
            if days > 0:
                return _advance_business_days(start_date, days - 1, week_mask=week_mask, holiday_dates=holiday_dates)
            return _advance_business_days(start_date, days + 1, week_mask=week_mask, holiday_dates=holiday_dates)
        if days > 0:
            if days == 1:
                return start_date
            first_business = _next_business_day(start_date, week_mask=week_mask, holiday_dates=holiday_dates)
            return _advance_business_days(first_business, days - 2, week_mask=week_mask, holiday_dates=holiday_dates)
        if days == -1:
            return start_date
        first_business = _previous_business_day(start_date, week_mask=week_mask, holiday_dates=holiday_dates)
        return _advance_business_days(first_business, days + 2, week_mask=week_mask, holiday_dates=holiday_dates)

    if is_business:
        if days == 0:
            return start_date
        return _advance_business_days(start_date, days, week_mask=week_mask, holiday_dates=holiday_dates)
    if days == 0:
        return _next_business_day(start_date, week_mask=week_mask, holiday_dates=holiday_dates)
    if days > 0:
        first_business = _next_business_day(start_date, week_mask=week_mask, holiday_dates=holiday_dates)
        return _advance_business_days(first_business, days - 1, week_mask=week_mask, holiday_dates=holiday_dates)
    first_business = _previous_business_day(start_date, week_mask=week_mask, holiday_dates=holiday_dates)
    return _advance_business_days(first_business, days + 1, week_mask=week_mask, holiday_dates=holiday_dates)


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
            "workspaces/example/output/docs.parquet",
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
            "workspaces/example/output/docs.xlsx",
            worksheet="Docs",
            table_name="docs",
            autofit=True,
        )

        df = pl.DataFrame({"claim_id": [3]})
        df.de.write_excel_atomic(target, worksheet="Docs")
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
        lf.de.sink_parquet_atomic("workspaces/example/output/docs.parquet")
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

    def remove_null_columns(self) -> pl.DataFrame:
        """Remove columns from this dataframe when every value is null.

        Returns
        -------
        pl.DataFrame
            Dataframe containing only columns with at least one non-null value.
        """
        return remove_null_columns(self._df)

    def networkdays(
        self,
        start: ExprLike,
        end: ExprLike,
        *,
        holidays: HolidayDates = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style business-day count expression for this dataframe.

        This is a convenience wrapper around :func:`data_engine.helpers.networkdays`.
        The returned value is still a normal ``pl.Expr``, so it can be chained
        into cumulative windows and other Polars expressions.

        Parameters
        ----------
        start : ExprLike
            Start date expression, column name, or scalar date/datetime.
        end : ExprLike
            End date expression, column name, or scalar date/datetime.
        holidays : HolidayDates
            Optional holiday dates removed from the business-day count.
        count_first_day : bool
            Whether to force the first day into the count when it would
            normally be excluded.
        mask : Iterable[bool] | None
            Monday-first seven-item business-day mask.

        Returns
        -------
        pl.Expr
            Expression that evaluates to the signed business-day count.

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
        holidays: HolidayDates = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style workday offset expression for this dataframe.

        This is a convenience wrapper around :func:`data_engine.helpers.workday`.

        Parameters
        ----------
        start : ExprLike
            Start date expression, column name, or scalar date/datetime.
        days : IntExprLike
            Signed business-day offset expression, column name, or scalar
            integer.
        holidays : HolidayDates
            Optional holiday dates skipped while calculating the result.
        count_first_day : bool
            Whether the start date itself can count as day 1.
        mask : Iterable[bool] | None
            Monday-first seven-item business-day mask.

        Returns
        -------
        pl.Expr
            Expression that evaluates to a ``Date`` result.

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

    def propagate_last_value(
        self,
        value: ColumnExpr,
        *,
        by: ColumnExprs,
        sort_by: ColumnExprs,
        where: pl.Expr | None = None,
        descending: DescendingLike = False,
        nulls_last: bool = False,
        ignore_nulls: bool = True,
    ) -> pl.Expr:
        """Return an expression broadcasting the last ordered value per window.

        This is a convenience wrapper around
        :func:`data_engine.helpers.propagate_last_value`.

        Parameters
        ----------
        value : ColumnExpr
            Column name or expression containing the value to propagate.
        by : ColumnExprs
            Window column or columns.
        sort_by : ColumnExprs
            Ordering column or columns used to define the last row in each
            window.
        where : pl.Expr | None
            Optional row predicate that limits which sorted rows can supply the
            propagated value.
        descending : DescendingLike
            Sort direction passed to ``Expr.sort_by``.
        nulls_last : bool
            Whether null sort-key values are ordered last.
        ignore_nulls : bool
            Whether null ``value`` rows are skipped before taking the last
            value.

        Returns
        -------
        pl.Expr
            Window expression suitable for ``with_columns`` or ``select``.
        """
        return propagate_last_value(
            value,
            by=by,
            sort_by=sort_by,
            where=where,
            descending=descending,
            nulls_last=nulls_last,
            ignore_nulls=ignore_nulls,
        )

    def visit_counter(
        self,
        value: ColumnExpr,
        *,
        by: ColumnExprs,
        sort_by: ColumnExprs,
        descending: DescendingLike = False,
        nulls_last: bool = False,
    ) -> pl.Expr:
        """Return a per-value contiguous-run visit number expression.

        This is a convenience wrapper around
        :func:`data_engine.helpers.visit_counter`.

        Parameters
        ----------
        value : ColumnExpr
            Column name or expression containing the state to count visits for.
        by : ColumnExprs
            Window column or columns.
        sort_by : ColumnExprs
            Ordering column or columns used to define row sequence inside each
            window.
        descending : DescendingLike
            Sort direction passed to ``DataFrame.sort`` for the in-window
            order.
        nulls_last : bool
            Whether null sort-key values are ordered last.

        Returns
        -------
        pl.Expr
            Unsigned integer expression containing the one-based visit number
            for each row's current ``value``.
        """
        return visit_counter(
            value,
            by=by,
            sort_by=sort_by,
            descending=descending,
            nulls_last=nulls_last,
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

    def remove_null_columns(self) -> pl.LazyFrame:
        """Remove columns from this lazy frame when every value is null.

        Returns
        -------
        pl.LazyFrame
            Lazy frame containing only columns with at least one non-null
            value.
        """
        trimmed = remove_null_columns(self._lf)
        assert isinstance(trimmed, pl.LazyFrame)
        return trimmed

    def networkdays(
        self,
        start: ExprLike,
        end: ExprLike,
        *,
        holidays: HolidayDates = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style business-day count expression for this lazy frame.

        This is a convenience wrapper around :func:`data_engine.helpers.networkdays`.
        The returned value stays lazy and can be chained into window
        expressions before ``collect()``.

        Parameters
        ----------
        start : ExprLike
            Start date expression, column name, or scalar date/datetime.
        end : ExprLike
            End date expression, column name, or scalar date/datetime.
        holidays : HolidayDates
            Optional holiday dates removed from the business-day count.
        count_first_day : bool
            Whether to force the first day into the count when it would
            normally be excluded.
        mask : Iterable[bool] | None
            Monday-first seven-item business-day mask.

        Returns
        -------
        pl.Expr
            Expression that evaluates to the signed business-day count.
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
        holidays: HolidayDates = None,
        count_first_day: bool = False,
        mask: Iterable[bool] | None = None,
    ) -> pl.Expr:
        """Return an Excel-style workday offset expression for this lazy frame.

        This is a convenience wrapper around :func:`data_engine.helpers.workday`.

        Parameters
        ----------
        start : ExprLike
            Start date expression, column name, or scalar date/datetime.
        days : IntExprLike
            Signed business-day offset expression, column name, or scalar
            integer.
        holidays : HolidayDates
            Optional holiday dates skipped while calculating the result.
        count_first_day : bool
            Whether the start date itself can count as day 1.
        mask : Iterable[bool] | None
            Monday-first seven-item business-day mask.

        Returns
        -------
        pl.Expr
            Expression that evaluates to a ``Date`` result.
        """
        return workday(
            start,
            days,
            holidays=holidays,
            count_first_day=count_first_day,
            mask=mask,
        )

    def propagate_last_value(
        self,
        value: ColumnExpr,
        *,
        by: ColumnExprs,
        sort_by: ColumnExprs,
        where: pl.Expr | None = None,
        descending: DescendingLike = False,
        nulls_last: bool = False,
        ignore_nulls: bool = True,
    ) -> pl.Expr:
        """Return an expression broadcasting the last ordered value per window.

        This is a convenience wrapper around
        :func:`data_engine.helpers.propagate_last_value`.

        Parameters
        ----------
        value : ColumnExpr
            Column name or expression containing the value to propagate.
        by : ColumnExprs
            Window column or columns.
        sort_by : ColumnExprs
            Ordering column or columns used to define the last row in each
            window.
        where : pl.Expr | None
            Optional row predicate that limits which sorted rows can supply the
            propagated value.
        descending : DescendingLike
            Sort direction passed to ``Expr.sort_by``.
        nulls_last : bool
            Whether null sort-key values are ordered last.
        ignore_nulls : bool
            Whether null ``value`` rows are skipped before taking the last
            value.

        Returns
        -------
        pl.Expr
            Window expression suitable for ``with_columns`` or ``select``.
        """
        return propagate_last_value(
            value,
            by=by,
            sort_by=sort_by,
            where=where,
            descending=descending,
            nulls_last=nulls_last,
            ignore_nulls=ignore_nulls,
        )

    def visit_counter(
        self,
        value: ColumnExpr,
        *,
        by: ColumnExprs,
        sort_by: ColumnExprs,
        descending: DescendingLike = False,
        nulls_last: bool = False,
    ) -> pl.Expr:
        """Return a per-value contiguous-run visit number expression.

        This is a convenience wrapper around
        :func:`data_engine.helpers.visit_counter`.

        Parameters
        ----------
        value : ColumnExpr
            Column name or expression containing the state to count visits for.
        by : ColumnExprs
            Window column or columns.
        sort_by : ColumnExprs
            Ordering column or columns used to define row sequence inside each
            window.
        descending : DescendingLike
            Sort direction passed to ``DataFrame.sort`` for the in-window
            order.
        nulls_last : bool
            Whether null sort-key values are ordered last.

        Returns
        -------
        pl.Expr
            Unsigned integer expression containing the one-based visit number
            for each row's current ``value``.
        """
        return visit_counter(
            value,
            by=by,
            sort_by=sort_by,
            descending=descending,
            nulls_last=nulls_last,
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

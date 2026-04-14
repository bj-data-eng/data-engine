"""Small schema helpers for Polars-oriented flow authoring."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import re
from typing import TypeAlias

import polars as pl

ColumnDtypes: TypeAlias = Mapping[str, object] | Iterable[tuple[str, object]]
ColumnRenames: TypeAlias = Mapping[str, str] | Iterable[tuple[str, str]]


class ColumnSelection(tuple[str, ...]):
    """Tuple-like column projection with Polars convenience methods.

    ``TableSchema.columns`` returns this type so schema definitions can be used
    directly in dataframe chains while still behaving like a normal tuple.

    Examples
    --------
    .. code-block:: python

        import polars as pl

        from data_engine.helpers import TableSchema

        schema = TableSchema(columns=("Claim Id",), dtypes={"Claim Id": pl.Int64})
        df = pl.DataFrame({"Claim Id": [1], "SSN": ["123"]})

        assert schema.columns.apply(df).columns == ["Claim Id"]
    """

    def __new__(cls, columns: Iterable[str]) -> "ColumnSelection":
        return super().__new__(cls, tuple(columns))

    def apply(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Select these columns from a Polars frame.

        Parameters
        ----------
        df : pl.DataFrame | pl.LazyFrame
            Eager or lazy Polars frame to transform.

        Returns
        -------
        pl.DataFrame | pl.LazyFrame
            Frame containing only these columns.
        """
        return df.select(tuple(self))

    def normalize_column_names(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Normalize this selection's column names on a Polars frame.

        Parameters
        ----------
        df : pl.DataFrame | pl.LazyFrame
            Eager or lazy Polars frame to rename.

        Returns
        -------
        pl.DataFrame | pl.LazyFrame
            Frame with matching selected column names normalized.
        """
        return normalize_column_names(df, columns=self)


class DropColumns(tuple[str, ...]):
    """Tuple-like drop list with a Polars ``apply`` helper.

    ``TableSchema.drop`` returns this type. Empty drop lists are no-ops, which
    keeps chained cleanup code simple.
    """

    def __new__(cls, columns: Iterable[str]) -> "DropColumns":
        return super().__new__(cls, tuple(columns))

    def apply(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Drop these columns from a Polars frame.

        Parameters
        ----------
        df : pl.DataFrame | pl.LazyFrame
            Eager or lazy Polars frame to transform.

        Returns
        -------
        pl.DataFrame | pl.LazyFrame
            Frame without these columns.
        """
        if not self:
            return df
        available = df.columns if isinstance(df, pl.DataFrame) else df.collect_schema().names()
        present = tuple(column for column in self if column in available)
        if not present:
            return df
        return df.drop(present)


class RenameColumns(dict[str, str]):
    """Dict-like rename mapping with a Polars ``apply`` helper.

    ``TableSchema.rename`` returns this type. Empty mappings are no-ops, so the
    same cleanup chain can be used whether a schema currently renames columns or
    not.
    """

    def apply(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Rename columns on a Polars frame.

        Parameters
        ----------
        df : pl.DataFrame | pl.LazyFrame
            Eager or lazy Polars frame to transform.

        Returns
        -------
        pl.DataFrame | pl.LazyFrame
            Frame with configured columns renamed.
        """
        if not self:
            return df
        return df.rename(dict(self))


class ColumnCasts(dict[str, object]):
    """Dict-like dtype mapping with a Polars ``apply`` helper.

    ``TableSchema.dtypes`` returns this type. Values are passed to
    ``polars.Expr.cast`` so callers can use normal Polars dtype objects such as
    ``pl.String``, ``pl.Int64``, and ``pl.Datetime``. ``apply`` casts remaining
    frame columns to ``pl.String``.
    """

    def _exprs(self, columns: Iterable[str]) -> tuple[pl.Expr, ...]:
        """Return Polars expressions for explicit casts plus string fallbacks."""
        return tuple(pl.col(column).cast(self.get(column, pl.String)) for column in columns)

    def apply(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Cast columns on a Polars frame.

        Parameters
        ----------
        df : pl.DataFrame | pl.LazyFrame
            Eager or lazy Polars frame to transform.

        Returns
        -------
        pl.DataFrame | pl.LazyFrame
            Frame with configured dtype casts applied and unspecified columns
            cast to ``pl.String``.
        """
        columns = df.columns if isinstance(df, pl.DataFrame) else df.collect_schema().names()
        if not columns:
            return df
        return df.with_columns(self._exprs(columns))


def _normalize_dtypes(dtypes: ColumnDtypes) -> dict[str, object]:
    if isinstance(dtypes, Mapping):
        items = dtypes.items()
    else:
        items = dtypes
    normalized: dict[str, object] = {}
    for column, dtype in items:
        column_name = str(column).strip()
        if not column_name:
            raise ValueError("TableSchema dtype column names must be non-empty.")
        if column_name in normalized:
            raise ValueError(f"Duplicate dtype column in TableSchema: {column_name!r}")
        normalized[column_name] = dtype
    return normalized


def _normalize_renames(rename: ColumnRenames) -> dict[str, str]:
    if isinstance(rename, Mapping):
        items = rename.items()
    else:
        items = rename
    normalized: dict[str, str] = {}
    for source, target in items:
        source_name = str(source).strip()
        target_name = str(target).strip()
        if not source_name or not target_name:
            raise ValueError("TableSchema rename column names must be non-empty.")
        if source_name in normalized:
            raise ValueError(f"Duplicate rename source in TableSchema: {source_name!r}")
        normalized[source_name] = target_name
    return normalized


def _normalize_drop(drop: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for column in drop:
        column_name = str(column).strip()
        if not column_name:
            raise ValueError("TableSchema drop column names must be non-empty.")
        if column_name in seen:
            raise ValueError(f"Duplicate drop column in TableSchema: {column_name!r}")
        normalized.append(column_name)
        seen.add(column_name)
    return tuple(normalized)


def _normalize_columns(columns: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for column in columns:
        column_name = str(column).strip()
        if not column_name:
            raise ValueError("TableSchema column names must be non-empty.")
        if column_name in seen:
            raise ValueError(f"Duplicate column in TableSchema columns: {column_name!r}")
        normalized.append(column_name)
        seen.add(column_name)
    return tuple(normalized)


def normalize_column_name(name: object) -> str:
    """Return a normalized column name.

    Parameters
    ----------
    name : object
        Source column name to normalize.

    Returns
    -------
    str
        Lowercase column name with separator-adjacent spaces removed, remaining
        whitespace collapsed, and spaces replaced with underscores.
    """
    text = str(name).strip()
    text = re.sub(r"\s*([#_\-/\\\\])\s*", r"\1", text)
    text = " ".join(text.split())
    text = text.replace(" ", "_")
    return text.lower()


def normalized_column_renames(columns: Iterable[object]) -> dict[str, str]:
    """Return a Polars rename mapping for normalized column names.

    Parameters
    ----------
    columns : Iterable[object]
        Source column names to normalize.

    Returns
    -------
    dict[str, str]
        Mapping from original column names to normalized names, excluding names
        that are already normalized.
    """
    renames: dict[str, str] = {}
    for column in columns:
        source = str(column)
        normalized = normalize_column_name(source)
        if source != normalized:
            renames[source] = normalized
    return renames


def normalize_column_names(
    df: pl.DataFrame | pl.LazyFrame,
    columns: Iterable[object] | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """Normalize column names on a Polars frame.

    Parameters
    ----------
    df : pl.DataFrame | pl.LazyFrame
        Eager or lazy Polars frame to rename.
    columns : Iterable[object] | None
        Optional subset of column names to normalize. When omitted, all frame
        columns are normalized.

    Returns
    -------
    pl.DataFrame | pl.LazyFrame
        Frame with normalized column names.
    """
    source_columns = df.columns if isinstance(df, pl.DataFrame) else df.collect_schema().names()
    target_columns = source_columns if columns is None else tuple(str(column) for column in columns)
    available_targets = [column for column in target_columns if column in source_columns]
    renames = normalized_column_renames(available_targets)
    if not renames:
        return df
    return df.rename(renames)


@dataclass(frozen=True)
class TableSchema:
    """Column cleanup helper for compact Polars dataframe chains.

    ``TableSchema`` is intentionally small: it stores an explicit column
    projection, a source-column dtype map, a rename map, and a drop list. Each
    attribute exposes an ``apply`` method so flow code can decide the cleanup
    order explicitly instead of relying on a magical all-in-one schema
    operation.

    Attributes
    ----------
    columns : Iterable[str] | ColumnSelection
        Explicit projection columns. Use ``schema.columns.apply(df)`` wherever
        that projection belongs in your chain.
    dtypes : ColumnDtypes | ColumnCasts
        Source column names mapped to Polars dtype objects. Use
        ``schema.dtypes.apply(df)`` to cast them. Remaining frame columns are
        cast to ``pl.String``.
    rename : ColumnRenames
        Source-to-target column names. Use ``schema.rename.apply(df)`` to rename
        them.
    drop : Iterable[str]
        Source columns to remove. Use ``schema.drop.apply(df)`` to drop them.

    Notes
    -----
    ``columns`` is an explicit projection applied at the point you call
    ``schema.columns.apply(df)``. For example, you might cast all incoming
    columns, drop private fields before persistence, write to DuckDB, and then
    select the columns to return.

    Examples
    --------
    .. code-block:: python

        import polars as pl

        from data_engine.helpers import TableSchema

        schema = TableSchema(
            columns=("step", "time", "workflow"),
            dtypes={"step_to": pl.String, "time": pl.Time},
            rename={"step_to": "step", "workflow_to": "workflow"},
            drop=("workflow_from", "ssn"),
        )

        df = pl.DataFrame(
            {
                "step_to": ["review"],
                "time": ["09:30:00"],
                "workflow_to": ["claims"],
                "workflow_from": ["intake"],
                "ssn": ["000-00-0000"],
            }
        ).with_columns(pl.col("time").str.to_time())

        df = schema.dtypes.apply(df)
        df = schema.drop.apply(df)
        df = schema.rename.apply(df)
        df = schema.columns.apply(df)

        assert df.columns == ["step", "time", "workflow"]
        assert df.schema["workflow"] == pl.String

    Normalize all incoming names first when source files use inconsistent
    spacing or capitalization:

    .. code-block:: python

        df = pl.DataFrame({"Workflow\\tTo": ["claims"]})
        df = schema.normalize_column_names(df)

        assert df.columns == ["workflow_to"]
    """

    columns: Iterable[str] | ColumnSelection = ()
    dtypes: ColumnDtypes | ColumnCasts = ()
    rename: ColumnRenames = ()
    drop: Iterable[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", ColumnSelection(_normalize_columns(self.columns)))
        object.__setattr__(self, "dtypes", ColumnCasts(_normalize_dtypes(self.dtypes)))
        object.__setattr__(self, "rename", RenameColumns(_normalize_renames(self.rename)))
        object.__setattr__(self, "drop", DropColumns(_normalize_drop(self.drop)))

    def normalize_column_names(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Normalize all column names on a Polars frame.

        Parameters
        ----------
        df : pl.DataFrame | pl.LazyFrame
            Eager or lazy Polars frame to rename.

        Returns
        -------
        pl.DataFrame | pl.LazyFrame
            Frame with normalized column names.
        """
        return normalize_column_names(df)


__all__ = [
    "ColumnCasts",
    "ColumnDtypes",
    "ColumnRenames",
    "ColumnSelection",
    "DropColumns",
    "RenameColumns",
    "TableSchema",
    "normalize_column_name",
    "normalize_column_names",
    "normalized_column_renames",
]

"""Small schema helpers for Polars-oriented flow authoring."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TypeAlias

import polars as pl

ColumnDtypes: TypeAlias = Mapping[str, object] | Iterable[tuple[str, object]]
ColumnRenames: TypeAlias = Mapping[str, str] | Iterable[tuple[str, str]]


class ColumnSelection(tuple[str, ...]):
    """Tuple-like column selection with a Polars ``apply`` helper."""

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
    """Tuple-like drop list with a Polars ``apply`` helper."""

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
        return df.drop(tuple(self))


class RenameColumns(dict[str, str]):
    """Dict-like rename mapping with a Polars ``apply`` helper."""

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
    """Dict-like dtype mapping with a Polars ``apply`` helper."""

    @property
    def _exprs(self) -> tuple[pl.Expr, ...]:
        """Return Polars expressions that cast configured source columns."""
        return tuple(pl.col(column).cast(dtype) for column, dtype in self.items())

    def apply(self, df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
        """Cast configured columns on a Polars frame.

        Parameters
        ----------
        df : pl.DataFrame | pl.LazyFrame
            Eager or lazy Polars frame to transform.

        Returns
        -------
        pl.DataFrame | pl.LazyFrame
            Frame with configured dtype casts applied.
        """
        if not self:
            return df
        return df.with_columns(self._exprs)


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


def _ordered_unique(*column_groups: Iterable[str]) -> tuple[str, ...]:
    columns: list[str] = []
    seen: set[str] = set()
    for group in column_groups:
        for column in group:
            if column in seen:
                continue
            columns.append(column)
            seen.add(column)
    return tuple(columns)


def normalize_column_name(name: object) -> str:
    """Return a normalized column name.

    Parameters
    ----------
    name : object
        Source column name to normalize.

    Returns
    -------
    str
        Lowercase column name with leading/trailing whitespace removed, internal
        whitespace collapsed, and spaces replaced with underscores.
    """
    return "_".join(str(name).strip().lower().split())


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
    """Column-selection helper for compact Polars cleanup chains."""

    dtypes: ColumnDtypes | ColumnCasts = ()
    rename: ColumnRenames = ()
    drop: Iterable[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "dtypes", ColumnCasts(_normalize_dtypes(self.dtypes)))
        object.__setattr__(self, "rename", RenameColumns(_normalize_renames(self.rename)))
        object.__setattr__(self, "drop", DropColumns(_normalize_drop(self.drop)))

    @property
    def columns(self) -> ColumnSelection:
        """Return source columns needed before drop/rename cleanup."""
        return ColumnSelection(_ordered_unique(self.dtypes.keys(), self.rename.keys(), self.drop))

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

"""Pure filter and sort helpers for dataframe preview popups."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

NULL_FILTER_VALUE = object()


@dataclass(frozen=True)
class PreviewSortState:
    """Immutable multi-column sort state for a dataframe preview.

    Args:
        columns: Ordered ``(column_name, descending)`` sort clauses.
    """

    columns: tuple[tuple[str, bool], ...] = ()

    def apply(self, column_name: str, *, descending: bool, append: bool) -> PreviewSortState:
        """Return state with a column sort applied.

        Args:
            column_name: Column to sort.
            descending: Whether the sort should be descending.
            append: Whether to append a new column to the active sort chain.

        Returns:
            Updated sort state.
        """

        normalized_column = str(column_name)
        updated_sorts = list(self.columns)
        existing_rank = self.rank_for(normalized_column)
        if existing_rank is not None:
            updated_sorts[existing_rank - 1] = (normalized_column, bool(descending))
        elif append:
            updated_sorts.append((normalized_column, bool(descending)))
        else:
            updated_sorts = [(normalized_column, bool(descending))]
        return PreviewSortState(tuple(updated_sorts))

    def clear(self) -> PreviewSortState:
        """Return empty sort state."""

        if not self.columns:
            return self
        return PreviewSortState()

    def remove(self, column_name: str) -> PreviewSortState:
        """Return state with a column removed from the sort chain.

        Args:
            column_name: Column to remove.

        Returns:
            Updated sort state.
        """

        normalized_column = str(column_name)
        updated_sorts = tuple(
            (active_name, active_descending)
            for active_name, active_descending in self.columns
            if active_name != normalized_column
        )
        if len(updated_sorts) == len(self.columns):
            return self
        return PreviewSortState(updated_sorts)

    def rank_for(self, column_name: str) -> int | None:
        """Return the one-based sort rank for a column, if active.

        Args:
            column_name: Column to inspect.

        Returns:
            One-based sort rank, or ``None`` when inactive.
        """

        for index, (active_name, _descending) in enumerate(self.columns, start=1):
            if active_name == column_name:
                return index
        return None

    def direction_for(self, column_name: str) -> bool | None:
        """Return whether a column sorts descending, if active.

        Args:
            column_name: Column to inspect.

        Returns:
            ``True`` for descending, ``False`` for ascending, or ``None`` when inactive.
        """

        for active_name, active_descending in self.columns:
            if active_name == column_name:
                return active_descending
        return None

    def primary_column(self) -> str | None:
        """Return the primary sort column, if any."""

        if not self.columns:
            return None
        return self.columns[0][0]


def build_distinct_value_filter_expression(
    column_name: str,
    selected_values: tuple[object, ...],
    *,
    dtype: pl.DataType | None = None,
):
    """Build a Polars expression for a preview distinct-value filter.

    Args:
        column_name: Column to filter.
        selected_values: Values selected by the popup.
        dtype: Optional column dtype used to preserve temporal precision.

    Returns:
        Polars expression for the selected values, or ``None`` when no values are selected.
    """

    if not selected_values:
        return None
    column = pl.col(column_name)
    include_null = any(value is NULL_FILTER_VALUE for value in selected_values)
    concrete_values = [value for value in selected_values if value is not NULL_FILTER_VALUE]
    expression = None
    if concrete_values:
        values = concrete_values if dtype is None else pl.Series(concrete_values, dtype=dtype).implode()
        expression = column.is_in(values)
    if include_null:
        null_expression = column.is_null()
        expression = null_expression if expression is None else (expression | null_expression)
    return expression


def should_clear_distinct_filter(
    selected_values: tuple[object, ...],
    all_values: tuple[object, ...],
    *,
    complete_domain: bool,
) -> bool:
    """Return whether selected values represent an inactive popup filter.

    Args:
        selected_values: Values selected by the popup.
        all_values: Values available in the popup list.
        complete_domain: Whether ``all_values`` covers the full column domain.

    Returns:
        ``True`` when the filter should be removed.
    """

    return not selected_values or (complete_domain and len(selected_values) == len(all_values))


def merge_selected_values(
    selected_values: tuple[object, ...],
    values: list[tuple[str, object]],
) -> list[tuple[str, object]]:
    """Merge active selected values in front of the loaded value list.

    Args:
        selected_values: Active selected values for a column.
        values: Loaded ``(label, value)`` rows from the preview or distinct-value query.

    Returns:
        Merged values with active selections first and duplicates removed.
    """

    if not selected_values:
        return values
    seen = set()
    merged: list[tuple[str, object]] = []
    for value in selected_values:
        label = "(blank)" if value is NULL_FILTER_VALUE else str(value)
        merged.append((label, value))
        seen.add(value_identity(value))
    for label, value in values:
        identity = value_identity(value)
        if identity in seen:
            continue
        merged.append((label, value))
        seen.add(identity)
    return merged


def value_identity(value: object) -> tuple[str, object]:
    """Return a stable identity for popup filter values."""

    if value is NULL_FILTER_VALUE:
        return ("null", "__blank__")
    return (type(value).__name__, value)

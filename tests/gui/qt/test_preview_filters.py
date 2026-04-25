from __future__ import annotations

from datetime import datetime

import polars as pl

from data_engine.ui.gui.rendering.preview_filters import (
    NULL_FILTER_VALUE,
    PreviewSortState,
    build_distinct_value_filter_expression,
    merge_selected_values,
    should_clear_distinct_filter,
)


def test_distinct_value_filter_expression_matches_selected_values_and_nulls() -> None:
    frame = pl.DataFrame({"status": ["open", "closed", None, "pending"]})

    result = frame.filter(
        build_distinct_value_filter_expression(
            "status",
            ("open", NULL_FILTER_VALUE),
            dtype=frame.schema["status"],
        )
    )

    assert result["status"].to_list() == ["open", None]


def test_distinct_value_filter_expression_preserves_datetime_time_unit_precision() -> None:
    timestamp = datetime(2026, 4, 24, 12, 30, 45, 123000)
    frame = pl.DataFrame({"created_at": [timestamp]}).with_columns(pl.col("created_at").cast(pl.Datetime("ms")))
    selected_value = frame.get_column("created_at").to_list()[0]

    result = frame.filter(
        build_distinct_value_filter_expression(
            "created_at",
            (selected_value,),
            dtype=frame.schema["created_at"],
        )
    )

    assert result.height == 1


def test_distinct_filter_clear_requires_complete_selected_domain() -> None:
    assert should_clear_distinct_filter((), ("a",), complete_domain=True) is True
    assert should_clear_distinct_filter(("a", "b"), ("a", "b"), complete_domain=True) is True
    assert should_clear_distinct_filter(("a", "b"), ("a", "b"), complete_domain=False) is False
    assert should_clear_distinct_filter(("a",), ("a", "b"), complete_domain=True) is False


def test_merge_selected_values_keeps_selected_values_first_without_duplicates() -> None:
    merged = merge_selected_values(
        ("missing", NULL_FILTER_VALUE, 1),
        [
            ("1", 1),
            ("(blank)", NULL_FILTER_VALUE),
            ("2", 2),
        ],
    )

    assert merged == [
        ("missing", "missing"),
        ("(blank)", NULL_FILTER_VALUE),
        ("1", 1),
        ("2", 2),
    ]


def test_preview_sort_state_applies_replaces_appends_and_removes_columns() -> None:
    state = PreviewSortState()

    state = state.apply("workflow", descending=False, append=False)
    state = state.apply("claim_id", descending=True, append=True)
    state = state.apply("workflow", descending=True, append=False)

    assert state.columns == (("workflow", True), ("claim_id", True))
    assert state.primary_column() == "workflow"
    assert state.rank_for("claim_id") == 2
    assert state.direction_for("workflow") is True

    state = state.remove("workflow")

    assert state.columns == (("claim_id", True),)
    assert state.rank_for("claim_id") == 1
    assert state.remove("missing") is state
    assert state.clear().columns == ()

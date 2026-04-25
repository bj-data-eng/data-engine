from __future__ import annotations

from datetime import date, datetime

import polars as pl

from data_engine.ui.gui.rendering.preview_filters import (
    ColumnFilter,
    NULL_FILTER_VALUE,
    PreviewSortState,
    build_column_filter_expression,
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


def test_column_filter_expression_compiles_distinct_filter() -> None:
    frame = pl.DataFrame({"status": ["open", "closed", None]})

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.distinct("status", ("closed", NULL_FILTER_VALUE)),
            dtype=frame.schema["status"],
        )
    )

    assert result["status"].to_list() == ["closed", None]


def test_column_filter_expression_compiles_text_operations() -> None:
    frame = pl.DataFrame({"status": ["open", "closed", "pending", None]})

    contains = frame.filter(build_column_filter_expression(ColumnFilter.text("status", "contains", "en")))
    not_contains = frame.filter(build_column_filter_expression(ColumnFilter.text("status", "not_contains", "en")))
    begins_with = frame.filter(build_column_filter_expression(ColumnFilter.text("status", "begins_with", "cl")))
    ends_with = frame.filter(build_column_filter_expression(ColumnFilter.text("status", "ends_with", "ing")))

    assert contains["status"].to_list() == ["open", "pending"]
    assert not_contains["status"].to_list() == ["closed", None]
    assert begins_with["status"].to_list() == ["closed"]
    assert ends_with["status"].to_list() == ["pending"]


def test_column_filter_expression_compiles_multiple_text_conditions() -> None:
    frame = pl.DataFrame({"status": ["open", "closed", "pending", "opened"]})

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.text_conditions(
                "status",
                (
                    ("contains", "open"),
                    ("not_equals", "open"),
                ),
            )
        )
    )

    assert result["status"].to_list() == ["opened"]


def test_column_filter_expression_combines_text_and_distinct_filters() -> None:
    frame = pl.DataFrame({"status": ["OPEN", "REOPENED", "PENDING", "CLOSED"]})

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.all(
                "status",
                (
                    ColumnFilter.text("status", "contains", "OPEN"),
                    ColumnFilter.distinct("status", ("REOPENED",)),
                ),
            ),
            dtype=frame.schema["status"],
        )
    )

    assert result["status"].to_list() == ["REOPENED"]


def test_column_filter_expression_compiles_date_ranges() -> None:
    frame = pl.DataFrame(
        {"created_on": [date(2026, 4, 23), date(2026, 4, 24), date(2026, 4, 25), date(2026, 4, 26)]}
    )

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.date_ranges(
                "created_on",
                (
                    ("2026-04-23", "2026-04-23"),
                    ("2026-04-25", "2026-04-26"),
                ),
            ),
            dtype=frame.schema["created_on"],
        )
    )

    assert result["created_on"].to_list() == [date(2026, 4, 23), date(2026, 4, 25), date(2026, 4, 26)]


def test_column_filter_expression_compiles_datetime_as_date() -> None:
    frame = pl.DataFrame(
        {
            "created_at": [
                datetime(2026, 4, 23, 23, 59),
                datetime(2026, 4, 24, 0, 1),
                datetime(2026, 4, 24, 23, 59),
            ]
        }
    ).with_columns(pl.col("created_at").cast(pl.Datetime("ms")))

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.date_range("created_at", "2026-04-24", "2026-04-24"),
            dtype=frame.schema["created_at"],
        )
    )

    assert result.height == 2


def test_column_filter_expression_compiles_microsecond_datetime_as_date() -> None:
    frame = pl.DataFrame(
        {
            "archived_at": [
                datetime(2026, 5, 9, 8, 24),
                datetime(2026, 5, 9, 16, 42),
                datetime(2026, 5, 10, 8, 24),
            ]
        }
    ).with_columns(pl.col("archived_at").cast(pl.Datetime("us")))

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.date_range("archived_at", "2026-05-09", "2026-05-09"),
            dtype=frame.schema["archived_at"],
        )
    )

    assert result.height == 2


def test_column_filter_expression_compiles_number_operations() -> None:
    frame = pl.DataFrame({"claim_id": [1001, 1002, 1003, 1004]})

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.number_conditions(
                "claim_id",
                (
                    ("greater_than_or_equal", "1002"),
                    ("less_than", "1004"),
                ),
            ),
            dtype=frame.schema["claim_id"],
        )
    )

    assert result["claim_id"].to_list() == [1002, 1003]


def test_column_filter_expression_compiles_float_number_filter() -> None:
    frame = pl.DataFrame({"amount": [10.5, 12.25, 15.75]})

    result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.number("amount", "greater_than", "12"),
            dtype=frame.schema["amount"],
        )
    )

    assert result["amount"].to_list() == [12.25, 15.75]


def test_column_filter_expression_compiles_boolean_filter() -> None:
    frame = pl.DataFrame({"is_ready": [True, False, None]})

    true_result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.boolean("is_ready", "true"),
            dtype=frame.schema["is_ready"],
        )
    )
    false_result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.boolean("is_ready", "false"),
            dtype=frame.schema["is_ready"],
        )
    )
    blank_result = frame.filter(
        build_column_filter_expression(
            ColumnFilter.boolean("is_ready", "blank"),
            dtype=frame.schema["is_ready"],
        )
    )

    assert true_result["is_ready"].to_list() == [True]
    assert false_result["is_ready"].to_list() == [False]
    assert blank_result["is_ready"].to_list() == [None]


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

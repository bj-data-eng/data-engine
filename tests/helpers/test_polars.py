from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from data_engine.helpers import networkdays
from data_engine.helpers import propagate_last_value
from data_engine.helpers import sink_parquet_atomic
from data_engine.helpers import visit_counter
from data_engine.helpers import workday
from data_engine.helpers import write_excel_atomic
from data_engine.helpers import write_parquet_atomic
from data_engine.helpers import polars as polars_helpers


def test_write_parquet_atomic_writes_and_replaces_target(tmp_path: Path):
    target = tmp_path / "nested" / "docs.parquet"
    old_frame = pl.DataFrame({"claim_id": [0]})
    new_frame = pl.DataFrame({"claim_id": [1, 2]})

    write_parquet_atomic(old_frame, target)
    returned_path = write_parquet_atomic(new_frame, target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), new_frame)
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_write_parquet_atomic_accepts_polars_write_options(tmp_path: Path):
    target = tmp_path / "docs.parquet"
    frame = pl.DataFrame({"claim_id": [1, 2]})

    returned_path = write_parquet_atomic(frame, target, compression="uncompressed", statistics=False)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_dataframe_namespace_exposes_atomic_write(tmp_path: Path):
    target = tmp_path / "docs.parquet"
    frame = pl.DataFrame({"claim_id": [1]})

    returned_path = frame.de.write_parquet_atomic(target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_write_excel_atomic_writes_and_replaces_target_with_polars_options(tmp_path: Path):
    target = tmp_path / "nested" / "docs.xlsx"
    old_frame = pl.DataFrame({"claim_id": [0]})
    new_frame = pl.DataFrame({"claim_id": [1, 2]})

    write_excel_atomic(old_frame, target, worksheet="Docs", table_name="docs_old")
    returned_path = write_excel_atomic(
        new_frame,
        target,
        worksheet="Docs",
        table_name="docs_new",
        autofit=True,
        include_header=True,
    )

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Docs"), new_frame)
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_dataframe_namespace_exposes_atomic_excel_write(tmp_path: Path):
    target = tmp_path / "docs.xlsx"
    frame = pl.DataFrame({"claim_id": [1]})

    returned_path = frame.de.write_excel_atomic(target, worksheet="Docs", table_name="docs")

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Docs"), frame)


def test_dataframe_namespace_normalizes_column_names():
    frame = pl.DataFrame({"Claim   Id": [1], "Workflow\tTo": ["docs"]})

    result = frame.de.normalize_column_names()

    assert result.columns == ["claim_id", "workflow_to"]


def test_sink_parquet_atomic_writes_lazy_frame(tmp_path: Path):
    target = tmp_path / "docs.parquet"
    frame = pl.DataFrame({"claim_id": [1, 2]})

    returned_path = sink_parquet_atomic(frame.lazy(), target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_lazyframe_namespace_exposes_atomic_sink(tmp_path: Path):
    target = tmp_path / "docs.parquet"
    frame = pl.DataFrame({"claim_id": [1, 2]})

    returned_path = frame.lazy().de.sink_parquet_atomic(target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_lazyframe_namespace_normalizes_column_names():
    lazy_frame = pl.DataFrame({"Claim   Id": [1], "Workflow\tTo": ["docs"]}).lazy()

    result = lazy_frame.de.normalize_column_names().collect()

    assert result.columns == ["claim_id", "workflow_to"]


def test_networkdays_matches_excel_style_inclusive_business_day_count():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13), date(2026, 4, 11), date(2026, 4, 14)],
            "end": [date(2026, 4, 14), date(2026, 4, 13), date(2026, 4, 13)],
        }
    ).select(networkdays("start", "end").alias("days"))

    assert result["days"].to_list() == [2, 1, -2]


def test_networkdays_excludes_holidays_from_list_inputs():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13)],
            "end": [date(2026, 4, 15)],
        }
    ).select(networkdays("start", "end", holidays=[date(2026, 4, 14)]).alias("days"))

    assert result["days"].to_list() == [2]


def test_networkdays_accepts_string_and_datetime_holidays_with_deduping():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13)],
            "end": [date(2026, 4, 15)],
        }
    ).select(
        networkdays(
            "start",
            "end",
            holidays=[
                "2026-04-14",
                datetime(2026, 4, 14, 8, 30),
                date(2026, 4, 14),
            ],
        ).alias("days")
    )

    assert result["days"].to_list() == [2]


def test_networkdays_count_first_day_forces_masked_or_holiday_start_into_count():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 11), date(2026, 4, 13), date(2026, 4, 15)],
            "end": [date(2026, 4, 13), date(2026, 4, 15), date(2026, 4, 13)],
        }
    ).select(
        regular=networkdays("start", "end", holidays=[date(2026, 4, 13)]),
        forced=networkdays("start", "end", holidays=[date(2026, 4, 13)], count_first_day=True),
    )

    assert result["regular"].to_list() == [0, 2, -2]
    assert result["forced"].to_list() == [1, 3, -2]


def test_networkdays_accepts_custom_mask():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13)],
            "end": [date(2026, 4, 19)],
        }
    ).select(networkdays("start", "end", mask=(True, True, True, True, False, False, True)).alias("days"))

    assert result["days"].to_list() == [5]


def test_networkdays_accepts_scalar_dates_and_namespace_helpers():
    frame = pl.DataFrame({"start": [date(2026, 4, 13)], "end": [date(2026, 4, 14)]})
    lazy_frame = frame.lazy()

    eager = frame.select(frame.de.networkdays("start", date(2026, 4, 14)).alias("days"))
    lazy = lazy_frame.select(lazy_frame.de.networkdays("start", "end").alias("days")).collect()

    assert eager["days"].to_list() == [2]
    assert lazy["days"].to_list() == [2]


def test_networkdays_honors_holidays_for_branch_derived_date_columns():
    frame = pl.DataFrame(
        {
            "process_flag": [True, True],
            "received_dt": [datetime(2026, 4, 13, 6, 45), datetime(2026, 4, 13, 18, 5)],
            "resolved_dt": [datetime(2026, 4, 15, 16, 0), datetime(2026, 4, 15, 18, 0)],
        }
    ).with_columns(
        received_date=pl.col("received_dt").dt.date(),
        resolved_date=pl.col("resolved_dt").dt.date(),
        received_time=pl.col("received_dt").dt.time(),
        resolved_time=pl.col("resolved_dt").dt.time(),
    ).with_columns(
        span_days=(pl.col("resolved_date") - pl.col("received_date")).dt.total_days(),
    ).with_columns(
        snapped_start=(
            pl.when(pl.col("received_time") > time(17, 0))
            .then(pl.col("received_date").dt.offset_by("1d"))
            .otherwise(pl.col("received_date"))
        ),
        snapped_end=(
            pl.when(
                (pl.col("received_time") <= time(7, 30))
                & (pl.col("resolved_time") < time(17, 0))
                & (pl.col("span_days") > 1)
            )
            .then(pl.col("resolved_date").dt.offset_by("-1d"))
            .otherwise(pl.col("resolved_date"))
        ),
    )

    result = frame.select(
        networkdays("snapped_start", "snapped_end", holidays=[date(2026, 4, 14)]).alias("days")
    )

    assert result["days"].to_list() == [1, 1]


def test_networkdays_returns_null_when_endpoint_is_null():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13), None],
            "end": [None, date(2026, 4, 14)],
        }
    ).select(networkdays("start", "end").alias("days"))

    assert result["days"].to_list() == [None, None]


def test_networkdays_rejects_invalid_mask_length():
    with pytest.raises(ValueError, match="exactly seven"):
        networkdays(date(2026, 4, 13), date(2026, 4, 14), mask=(True, False))


def test_networkdays_rejects_non_boolean_mask_values():
    with pytest.raises(TypeError, match="exactly seven"):
        networkdays(
            date(2026, 4, 13),
            date(2026, 4, 14),
            mask=("True", "True", "True", "True", "True", "False", "False"),
        )


def test_networkdays_rejects_invalid_holiday_values():
    with pytest.raises(TypeError, match="holidays must contain"):
        networkdays(
            date(2026, 4, 13),
            date(2026, 4, 14),
            holidays=[123],
        )


def test_networkdays_rejects_invalid_holiday_strings():
    with pytest.raises(ValueError):
        networkdays(
            date(2026, 4, 13),
            date(2026, 4, 14),
            holidays=["04/14/2026"],
        )


def test_workday_matches_excel_style_offsets_for_business_and_weekend_starts():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13), date(2026, 4, 11), date(2026, 4, 13), date(2026, 4, 11)],
            "days": [1, 1, -1, 0],
        }
    ).select(workday("start", "days").alias("target"))

    assert result["target"].to_list() == [
        date(2026, 4, 14),
        date(2026, 4, 13),
        date(2026, 4, 10),
        date(2026, 4, 13),
    ]


def test_workday_excludes_holidays_from_offsets():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13)],
            "days": [2],
        }
    ).select(workday("start", "days", holidays=[date(2026, 4, 14)]).alias("target"))

    assert result["target"].to_list() == [date(2026, 4, 16)]


def test_workday_count_first_day_allows_start_date_to_be_day_one():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13), date(2026, 4, 11), date(2026, 4, 13)],
            "days": [1, 1, 2],
        }
    ).select(
        regular=workday("start", "days", holidays=[date(2026, 4, 13)]),
        forced=workday("start", "days", holidays=[date(2026, 4, 13)], count_first_day=True),
    )

    assert result["regular"].to_list() == [
        date(2026, 4, 14),
        date(2026, 4, 14),
        date(2026, 4, 15),
    ]
    assert result["forced"].to_list() == [
        date(2026, 4, 13),
        date(2026, 4, 11),
        date(2026, 4, 14),
    ]


def test_workday_accepts_custom_mask_and_namespace_helpers():
    frame = pl.DataFrame({"start": [date(2026, 4, 17)], "days": [1]})
    mask = (True, True, True, True, False, False, True)

    eager = frame.select(frame.de.workday("start", "days", mask=mask).alias("target"))
    lazy = frame.lazy().select(pl.all(), pl.lit(1).alias("x")).select(
        pl.col("start"),
        pl.col("days"),
        frame.lazy().de.workday("start", "days", mask=mask).alias("target"),
    ).collect()

    assert eager["target"].to_list() == [date(2026, 4, 19)]
    assert lazy["target"].to_list() == [date(2026, 4, 19)]


def test_workday_zero_offset_on_non_business_day_rolls_to_next_business_day_for_custom_mask():
    frame = pl.DataFrame({"start": [date(2015, 8, 23)], "days": [0]})
    mask = (False, True, True, True, True, False, False)

    result = frame.select(workday("start", "days", mask=mask).alias("target"))

    assert result["target"].to_list() == [date(2015, 8, 25)]


def test_workday_returns_null_when_input_is_null():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13), None],
            "days": [None, 1],
        }
    ).select(workday("start", "days").alias("target"))

    assert result["target"].to_list() == [None, None]


def test_workday_rejects_non_boolean_mask_values():
    with pytest.raises(TypeError, match="exactly seven"):
        workday(
            date(2026, 4, 13),
            1,
            mask=(1, 1, 1, 1, 1, 0, 0),
        )


def test_workday_accepts_string_and_datetime_holidays_with_deduping():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13)],
            "days": [2],
        }
    ).select(
        workday(
            "start",
            "days",
            holidays=[
                "2026-04-14",
                datetime(2026, 4, 14, 12, 0),
                date(2026, 4, 14),
            ],
        ).alias("target")
    )

    assert result["target"].to_list() == [date(2026, 4, 16)]


def test_propagate_last_value_broadcasts_latest_non_null_value_per_window():
    frame = pl.DataFrame(
        {
            "claim_id": ["a", "a", "a", "b", "b", "c"],
            "step_index": [1, 2, 3, 1, 2, 1],
            "status": [None, "ready", None, "open", "closed", None],
        }
    )

    result = frame.with_columns(
        latest_status=propagate_last_value(
            "status",
            by="claim_id",
            sort_by="step_index",
        )
    )

    assert result.to_dict(as_series=False)["latest_status"] == [
        "ready",
        "ready",
        "ready",
        "closed",
        "closed",
        None,
    ]


def test_propagate_last_value_accepts_multiple_window_and_sort_columns():
    frame = pl.DataFrame(
        {
            "claim_id": ["a", "a", "a", "a", "b"],
            "line": [1, 1, 1, 2, 1],
            "step_index": [1, 2, 2, 1, 1],
            "tie_breaker": [1, 1, 2, 1, 1],
            "status": [None, "first", "last", "line-two", "only"],
        }
    )

    result = frame.with_columns(
        latest_status=propagate_last_value(
            "status",
            by=["claim_id", "line"],
            sort_by=["step_index", "tie_breaker"],
        )
    )

    assert result.to_dict(as_series=False)["latest_status"] == [
        "last",
        "last",
        "last",
        "line-two",
        "only",
    ]


def test_propagate_last_value_namespace_helpers_work_for_eager_and_lazy_frames():
    frame = pl.DataFrame(
        {
            "claim_id": ["a", "a", "b", "b"],
            "step_index": [1, 2, 1, 2],
            "status": ["open", None, "queued", "done"],
        }
    )

    eager = frame.with_columns(
        latest_status=frame.de.propagate_last_value("status", by="claim_id", sort_by="step_index")
    )
    lazy_frame = frame.lazy()
    lazy = lazy_frame.with_columns(
        latest_status=lazy_frame.de.propagate_last_value("status", by="claim_id", sort_by="step_index")
    ).collect()

    assert eager.to_dict(as_series=False)["latest_status"] == ["open", "open", "done", "done"]
    assert lazy.to_dict(as_series=False)["latest_status"] == ["open", "open", "done", "done"]


def test_propagate_last_value_can_keep_nulls_when_requested():
    frame = pl.DataFrame(
        {
            "claim_id": ["a", "a", "b", "b"],
            "step_index": [1, 2, 1, 2],
            "status": ["open", None, None, "done"],
        }
    )

    result = frame.with_columns(
        latest_status=propagate_last_value(
            "status",
            by="claim_id",
            sort_by="step_index",
            ignore_nulls=False,
        )
    )

    assert result.to_dict(as_series=False)["latest_status"] == [None, None, "done", "done"]


def test_propagate_last_value_can_filter_source_rows_and_return_adjacent_values():
    frame = pl.DataFrame(
        {
            "claim_id": ["a", "a", "a", "b", "b"],
            "step_index": [1, 2, 3, 1, 2],
            "event": ["Open", "Archive", "Archive", "Open", "Archive"],
            "event_date": ["2026-04-01", "2026-04-02", "2026-04-03", "2026-05-01", "2026-05-02"],
            "event_time": ["08:00", "09:30", "10:15", "11:00", "12:45"],
        }
    )

    result = frame.with_columns(
        archived_at=propagate_last_value(
            pl.concat_str(["event_date", "event_time"], separator=" "),
            by="claim_id",
            sort_by="step_index",
            where=pl.col("event") == "Archive",
        )
    )

    assert result.to_dict(as_series=False)["archived_at"] == [
        "2026-04-03 10:15",
        "2026-04-03 10:15",
        "2026-04-03 10:15",
        "2026-05-02 12:45",
        "2026-05-02 12:45",
    ]


def test_visit_counter_counts_repeated_contiguous_value_runs_per_window():
    frame = pl.DataFrame(
        {
            "document_id": ["doc-1"] * 8,
            "step_index": [1, 2, 3, 4, 5, 6, 7, 8],
            "workflow": ["w1", "w1", "w1", "w2", "w2", "w1", "w1", "w1"],
        }
    )

    result = frame.with_columns(
        workflow_visit=visit_counter("workflow", by="document_id", sort_by="step_index")
    )

    assert result.to_dict(as_series=False)["workflow_visit"] == [1, 1, 1, 1, 1, 2, 2, 2]


def test_visit_counter_maps_results_back_to_original_row_order():
    frame = pl.DataFrame(
        {
            "document_id": ["doc-1"] * 5,
            "step_index": [5, 1, 4, 2, 3],
            "workflow": ["w1", "w1", "w2", "w1", "w2"],
        }
    )

    result = frame.with_columns(
        workflow_visit=visit_counter("workflow", by="document_id", sort_by="step_index")
    )

    assert result.to_dict(as_series=False)["workflow_visit"] == [2, 1, 1, 1, 1]


def test_visit_counter_accepts_multiple_window_and_sort_columns():
    frame = pl.DataFrame(
        {
            "document_id": ["doc-1", "doc-1", "doc-1", "doc-1", "doc-1", "doc-2"],
            "section": ["a", "a", "a", "a", "b", "a"],
            "step_index": [1, 2, 2, 3, 1, 1],
            "tie_breaker": [1, 1, 2, 1, 1, 1],
            "workflow": ["w1", "w2", "w1", "w1", "w1", "w1"],
        }
    )

    result = frame.with_columns(
        workflow_visit=visit_counter(
            "workflow",
            by=["document_id", "section"],
            sort_by=["step_index", "tie_breaker"],
        )
    )

    assert result.to_dict(as_series=False)["workflow_visit"] == [1, 1, 2, 2, 1, 1]


def test_visit_counter_namespace_helpers_work_for_eager_and_lazy_frames():
    frame = pl.DataFrame(
        {
            "document_id": ["doc-1", "doc-1", "doc-1"],
            "step_index": [1, 2, 3],
            "workflow": ["w1", "w2", "w1"],
        }
    )

    eager = frame.with_columns(
        workflow_visit=frame.de.visit_counter("workflow", by="document_id", sort_by="step_index")
    )
    lazy_frame = frame.lazy()
    lazy = lazy_frame.with_columns(
        workflow_visit=lazy_frame.de.visit_counter("workflow", by="document_id", sort_by="step_index")
    ).collect()

    assert eager.to_dict(as_series=False)["workflow_visit"] == [1, 1, 2]
    assert lazy.to_dict(as_series=False)["workflow_visit"] == [1, 1, 2]


def test_atomic_write_cleans_temporary_file_and_preserves_target_on_replace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    target = tmp_path / "docs.parquet"
    old_frame = pl.DataFrame({"claim_id": [0]})
    new_frame = pl.DataFrame({"claim_id": [1]})
    write_parquet_atomic(old_frame, target)

    def fail_replace(source: Path, destination: Path) -> None:
        raise RuntimeError(f"replace failed for {source} -> {destination}")

    monkeypatch.setattr(polars_helpers.os, "replace", fail_replace)

    with pytest.raises(RuntimeError, match="replace failed"):
        write_parquet_atomic(new_frame, target)

    assert_frame_equal(pl.read_parquet(target), old_frame)
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []


def test_sink_parquet_atomic_requires_eager_sink(tmp_path: Path):
    target = tmp_path / "docs.parquet"

    with pytest.raises(ValueError, match="eager sink"):
        sink_parquet_atomic(pl.DataFrame({"claim_id": [1]}).lazy(), target, lazy=True)


def test_dataframe_namespace_builds_and_attaches_dimension(tmp_path: Path):
    db_path = tmp_path / "docs.duckdb"
    frame = pl.DataFrame(
        {
            "member_id": ["a", "a", "b"],
            "lob": ["medical", "medical", "dental"],
            "amount": [10, 20, 30],
        }
    )

    mapping = frame.select(["member_id", "lob"]).unique(maintain_order=True).de.build_dimension(
        db_path,
        "dim_member",
        key_column="member_key",
    )
    attached = frame.de.attach_dimension(
        db_path,
        "dim_member",
        on=["member_id", "lob"],
        key_column="member_key",
    )

    assert mapping.to_dict(as_series=False) == {
        "member_id": ["a", "b"],
        "lob": ["medical", "dental"],
        "member_key": [1, 2],
    }
    assert attached.to_dict(as_series=False) == {
        "member_id": ["a", "a", "b"],
        "lob": ["medical", "medical", "dental"],
        "amount": [10, 20, 30],
        "member_key": [1, 1, 2],
    }


def test_dataframe_namespace_normalizes_and_denormalizes_columns(tmp_path: Path):
    db_path = tmp_path / "docs.duckdb"
    frame = pl.DataFrame(
        {
            "member_id": ["a", "a", "b"],
            "lob": ["medical", "medical", "dental"],
            "amount": [10, 20, 30],
        }
    )

    normalized = frame.de.normalize_columns(
        db_path,
        "dim_member",
        on=["member_id", "lob"],
        key_column="member_key",
    )
    denormalized = normalized.de.denormalize_columns(
        db_path,
        "dim_member",
        key_column="member_key",
        select=["member_id", "lob"],
        drop_key=True,
    )

    assert normalized.to_dict(as_series=False) == {
        "amount": [10, 20, 30],
        "member_key": [1, 1, 2],
    }
    assert denormalized.to_dict(as_series=False) == {
        "amount": [10, 20, 30],
        "member_id": ["a", "a", "b"],
        "lob": ["medical", "medical", "dental"],
    }


def test_dataframe_namespace_replaces_duckdb_rows(tmp_path: Path):
    db_path = tmp_path / "docs.duckdb"

    returned = pl.DataFrame({"claim_id": [1], "amount": [10]}).de.replace_rows_by_file(
        db_path,
        "fact_claim",
        file_hash="file-a",
    )
    replaced = pl.DataFrame({"claim_id": [1], "amount": [20]}).de.replace_rows_by_values(
        db_path,
        "fact_claim",
        column="claim_id",
    )
    table = pl.DataFrame({"claim_id": [3], "amount": [30]}).de.replace_table(db_path, "fact_claim")

    assert returned.to_dict(as_series=False) == {
        "claim_id": [1],
        "amount": [10],
        "file_key": ["file-a"],
    }
    assert replaced.to_dict(as_series=False) == {"claim_id": [1], "amount": [20]}
    assert table.to_dict(as_series=False) == {"claim_id": [3], "amount": [30]}


def test_lazyframe_namespace_wraps_duckdb_helpers(tmp_path: Path):
    db_path = tmp_path / "docs.duckdb"
    lazy_frame = pl.DataFrame({"status": ["open", "ready", "open"], "amount": [10, 20, 30]}).lazy()

    normalized = lazy_frame.de.normalize_columns(
        db_path,
        "dim_status",
        on="status",
        key_column="status_key",
    )
    replaced = lazy_frame.de.replace_table(db_path, "fact_claim")

    assert normalized.to_dict(as_series=False) == {
        "amount": [10, 20, 30],
        "status_key": [1, 2, 1],
    }
    assert replaced.to_dict(as_series=False) == {
        "status": ["open", "ready", "open"],
        "amount": [10, 20, 30],
    }


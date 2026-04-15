from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from data_engine.helpers import networkdays
from data_engine.helpers import sink_parquet_atomic
from data_engine.helpers import workday
from data_engine.helpers import write_excel_atomic
from data_engine.helpers import write_parquet_atomic
from data_engine.helpers import polars as polars_helpers


def test_write_parquet_atomic_writes_and_replaces_target(tmp_path: Path):
    target = tmp_path / "nested" / "claims.parquet"
    old_frame = pl.DataFrame({"claim_id": [0]})
    new_frame = pl.DataFrame({"claim_id": [1, 2]})

    write_parquet_atomic(old_frame, target)
    returned_path = write_parquet_atomic(new_frame, target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), new_frame)
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_write_parquet_atomic_accepts_polars_write_options(tmp_path: Path):
    target = tmp_path / "claims.parquet"
    frame = pl.DataFrame({"claim_id": [1, 2]})

    returned_path = write_parquet_atomic(frame, target, compression="uncompressed", statistics=False)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_dataframe_namespace_exposes_atomic_write(tmp_path: Path):
    target = tmp_path / "claims.parquet"
    frame = pl.DataFrame({"claim_id": [1]})

    returned_path = frame.de.write_parquet_atomic(target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_write_excel_atomic_writes_and_replaces_target_with_polars_options(tmp_path: Path):
    target = tmp_path / "nested" / "claims.xlsx"
    old_frame = pl.DataFrame({"claim_id": [0]})
    new_frame = pl.DataFrame({"claim_id": [1, 2]})

    write_excel_atomic(old_frame, target, worksheet="Claims", table_name="claims_old")
    returned_path = write_excel_atomic(
        new_frame,
        target,
        worksheet="Claims",
        table_name="claims_new",
        autofit=True,
        include_header=True,
    )

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), new_frame)
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_dataframe_namespace_exposes_atomic_excel_write(tmp_path: Path):
    target = tmp_path / "claims.xlsx"
    frame = pl.DataFrame({"claim_id": [1]})

    returned_path = frame.de.write_excel_atomic(target, worksheet="Claims", table_name="claims")

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), frame)


def test_dataframe_namespace_normalizes_column_names():
    frame = pl.DataFrame({"Claim   Id": [1], "Workflow\tTo": ["claims"]})

    result = frame.de.normalize_column_names()

    assert result.columns == ["claim_id", "workflow_to"]


def test_sink_parquet_atomic_writes_lazy_frame(tmp_path: Path):
    target = tmp_path / "claims.parquet"
    frame = pl.DataFrame({"claim_id": [1, 2]})

    returned_path = sink_parquet_atomic(frame.lazy(), target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_lazyframe_namespace_exposes_atomic_sink(tmp_path: Path):
    target = tmp_path / "claims.parquet"
    frame = pl.DataFrame({"claim_id": [1, 2]})

    returned_path = frame.lazy().de.sink_parquet_atomic(target)

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_parquet(target), frame)


def test_lazyframe_namespace_normalizes_column_names():
    lazy_frame = pl.DataFrame({"Claim   Id": [1], "Workflow\tTo": ["claims"]}).lazy()

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


def test_workday_returns_null_when_input_is_null():
    result = pl.DataFrame(
        {
            "start": [date(2026, 4, 13), None],
            "days": [None, 1],
        }
    ).select(workday("start", "days").alias("target"))

    assert result["target"].to_list() == [None, None]


def test_atomic_write_cleans_temporary_file_and_preserves_target_on_replace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    target = tmp_path / "claims.parquet"
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
    target = tmp_path / "claims.parquet"

    with pytest.raises(ValueError, match="eager sink"):
        sink_parquet_atomic(pl.DataFrame({"claim_id": [1]}).lazy(), target, lazy=True)


def test_dataframe_namespace_builds_and_attaches_dimension(tmp_path: Path):
    db_path = tmp_path / "claims.duckdb"
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
    db_path = tmp_path / "claims.duckdb"
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
    db_path = tmp_path / "claims.duckdb"

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
    db_path = tmp_path / "claims.duckdb"
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

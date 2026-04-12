from __future__ import annotations

from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from data_engine.helpers import sink_parquet_atomic
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

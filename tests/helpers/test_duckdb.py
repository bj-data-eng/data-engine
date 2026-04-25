from __future__ import annotations


import duckdb
import polars as pl
import pytest

from data_engine.helpers import duckdb as duckdb_helpers
from data_engine.helpers.duckdb import attach_dimension
from data_engine.helpers.duckdb import build_dimension
from data_engine.helpers.duckdb import compact_database
from data_engine.helpers.duckdb import denormalize_columns
from data_engine.helpers.duckdb import ensure_index
from data_engine.helpers.duckdb import normalize_columns
from data_engine.helpers.duckdb import read_rows_by_values
from data_engine.helpers.duckdb import read_sql
from data_engine.helpers.duckdb import read_table
from data_engine.helpers.duckdb import replace_rows_by_file
from data_engine.helpers.duckdb import replace_rows_by_values
from data_engine.helpers.duckdb import replace_table


def test_build_dimension_creates_dimension_and_returns_unique_mapping(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    incoming = pl.DataFrame(
        {
            "member_id": ["a", "a", "b"],
            "lob": ["medical", "medical", "dental"],
        }
    )

    mapping = build_dimension(
        db_path,
        "dim_member",
        df=incoming,
        key_column="member_key",
    )

    assert mapping.to_dict(as_series=False) == {
        "member_id": ["a", "b"],
        "lob": ["medical", "dental"],
        "member_key": [1, 2],
    }

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "member_id", "lob", "member_key" FROM "dim_member" ORDER BY "member_id", "lob"'
        ).pl()

    assert persisted.to_dict(as_series=False) == mapping.to_dict(as_series=False)


def test_build_dimension_quotes_keyword_columns_and_preserves_existing_keys(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    first = pl.DataFrame(
        {
            "group": ["docs", "eligibility"],
            "status": ["open", "ready"],
        }
    )
    second = pl.DataFrame(
        {
            "group": ["eligibility", "payments"],
            "status": ["ready", "queued"],
        }
    )

    first_mapping = build_dimension(
        db_path,
        "mart.dim_group",
        df=first,
        key_column="group_key",
    )
    second_mapping = build_dimension(
        db_path,
        "mart.dim_group",
        df=second,
        key_column="group_key",
    )

    assert first_mapping.to_dict(as_series=False) == {
        "group": ["docs", "eligibility"],
        "status": ["open", "ready"],
        "group_key": [1, 2],
    }
    assert second_mapping.to_dict(as_series=False) == {
        "group": ["eligibility", "payments"],
        "status": ["ready", "queued"],
        "group_key": [2, 3],
    }


def test_build_dimension_allows_side_effect_only_calls(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    incoming = pl.DataFrame({"category": ["a", "b", "a"]})

    result = build_dimension(
        db_path,
        "dim_category",
        df=incoming,
        return_df=False,
    )

    assert result is None

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "category", "dimension_key" FROM "dim_category" ORDER BY "category"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "category": ["a", "b"],
        "dimension_key": [1, 2],
    }


def test_build_dimension_collects_lazy_frames(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    incoming = pl.DataFrame({"member_id": ["a", "a", "b"], "lob": ["medical", "medical", "dental"]}).lazy()

    mapping = build_dimension(
        db_path,
        "dim_member",
        df=incoming,
        key_column="member_key",
    )

    assert mapping.to_dict(as_series=False) == {
        "member_id": ["a", "b"],
        "lob": ["medical", "dental"],
        "member_key": [1, 2],
    }


def test_build_dimension_rejects_key_column_collisions(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    incoming = pl.DataFrame({"dimension_key": [1], "category": ["a"]})

    try:
        build_dimension(db_path, "dim_category", df=incoming)
    except ValueError as exc:
        assert "must not already exist" in str(exc)
    else:
        raise AssertionError("Expected build_dimension() to reject incoming key-column collisions.")


def test_build_dimension_rolls_back_and_closes_connection_on_failure(monkeypatch, tmp_path):
    real_connect = duckdb.connect
    events: list[str] = []

    class _ConnectionProxy:
        def __init__(self, inner):
            self.inner = inner

        def execute(self, sql: str, *args, **kwargs):
            statement = str(sql).strip().upper()
            if statement.startswith("BEGIN"):
                events.append("begin")
            elif statement.startswith("ROLLBACK"):
                events.append("rollback")
            elif statement.startswith("COMMIT"):
                events.append("commit")
            if 'CREATE UNIQUE INDEX IF NOT EXISTS "uq_dim_' in sql:
                raise RuntimeError("boom")
            return self.inner.execute(sql, *args, **kwargs)

        def register(self, *args, **kwargs):
            return self.inner.register(*args, **kwargs)

        def close(self):
            events.append("close")
            return self.inner.close()

    def _connect(path):
        return _ConnectionProxy(real_connect(path))

    monkeypatch.setattr(duckdb_helpers.duckdb, "connect", _connect)

    with pytest.raises(RuntimeError, match="boom"):
        build_dimension(
            tmp_path / "docs.duckdb",
            "dim_category",
            df=pl.DataFrame({"category": ["a"]}),
        )

    assert events == ["begin", "rollback", "close"]


def test_denormalize_columns_attaches_natural_columns_by_surrogate_key(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    build_dimension(
        db_path,
        "dim_member",
        df=pl.DataFrame({"member_id": ["a", "b"], "lob": ["medical", "dental"]}),
        key_column="member_key",
    )

    keyed = pl.DataFrame({"claim_id": [10, 11], "member_key": [2, 1], "amount": [25, 50]})

    denormalized = denormalize_columns(
        db_path,
        "dim_member",
        df=keyed,
        key_column="member_key",
    )

    assert denormalized.to_dict(as_series=False) == {
        "claim_id": [10, 11],
        "member_key": [2, 1],
        "amount": [25, 50],
        "member_id": ["b", "a"],
        "lob": ["dental", "medical"],
    }


def test_denormalize_columns_can_select_subset_and_drop_surrogate_key(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    build_dimension(
        db_path,
        "mart.dim_group",
        df=pl.DataFrame({"group": ["docs", "payments"], "status": ["open", "queued"]}),
        key_column="group_key",
    )

    keyed = pl.DataFrame({"group_key": [1, 2], "count": [3, 4]})

    denormalized = denormalize_columns(
        db_path,
        "mart.dim_group",
        df=keyed,
        key_column="group_key",
        select=["group"],
        drop_key=True,
    )

    assert denormalized.to_dict(as_series=False) == {
        "count": [3, 4],
        "group": ["docs", "payments"],
    }


def test_replace_rows_by_file_creates_table_and_returns_frame_with_file_key(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    incoming = pl.DataFrame({"claim_id": [1, 2], "amount": [10, 20]})

    returned = replace_rows_by_file(
        db_path,
        "fact_claim",
        df=incoming,
        file_hash="file-a",
    )

    assert returned.to_dict(as_series=False) == {
        "claim_id": [1, 2],
        "amount": [10, 20],
        "file_key": ["file-a", "file-a"],
    }

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "amount", "file_key" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == returned.to_dict(as_series=False)


def test_replace_rows_by_file_replaces_one_file_slice_without_touching_other_files(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1, 2], "amount": [10, 20]}),
        file_hash="file-a",
    )
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [3], "amount": [30]}),
        file_hash="file-b",
    )
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1, 4], "amount": [100, 400]}),
        file_hash="file-a",
    )

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "amount", "file_key" FROM "fact_claim" ORDER BY "file_key", "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [1, 4, 3],
        "amount": [100, 400, 30],
        "file_key": ["file-a", "file-a", "file-b"],
    }


def test_replace_rows_by_file_expands_schema_for_new_columns(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [2], "amount": [20], "group": ["docs"]}),
        file_hash="file-b",
    )

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "amount", "group", "file_key" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [1, 2],
        "amount": [10, 20],
        "group": [None, "docs"],
        "file_key": ["file-a", "file-b"],
    }


def test_replace_rows_by_file_allows_side_effect_only_calls(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    result = replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1]}),
        file_hash="file-a",
        return_df=False,
    )

    assert result is None


def test_replace_rows_by_file_is_not_dependent_on_df_column_order(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"amount": [20], "claim_id": [2]}),
        file_hash="file-b",
    )

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "amount", "file_key" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [1, 2],
        "amount": [10, 20],
        "file_key": ["file-a", "file-b"],
    }


def test_replace_rows_by_file_does_not_copy_incoming_to_temp_table(monkeypatch, tmp_path):
    real_connect = duckdb.connect

    class _ConnectionProxy:
        def __init__(self, inner):
            self.inner = inner

        def execute(self, sql: str, *args, **kwargs):
            statement = str(sql).strip().upper()
            assert "__DATA_ENGINE_INCREMENTAL_INCOMING_TABLE" not in statement
            return self.inner.execute(sql, *args, **kwargs)

        def register(self, *args, **kwargs):
            return self.inner.register(*args, **kwargs)

        def close(self):
            return self.inner.close()

    def _connect(path):
        return _ConnectionProxy(real_connect(path))

    monkeypatch.setattr(duckdb_helpers.duckdb, "connect", _connect)

    replace_rows_by_file(
        tmp_path / "docs.duckdb",
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )


def test_replace_rows_by_file_rolls_back_and_closes_connection_on_failure(monkeypatch, tmp_path):
    real_connect = duckdb.connect
    events: list[str] = []

    class _ConnectionProxy:
        def __init__(self, inner):
            self.inner = inner

        def execute(self, sql: str, *args, **kwargs):
            statement = str(sql).strip().upper()
            if statement.startswith("BEGIN"):
                events.append("begin")
            elif statement.startswith("ROLLBACK"):
                events.append("rollback")
            elif statement.startswith("COMMIT"):
                events.append("commit")
            if "ALTER TABLE" in statement and '"GROUP"' in statement:
                raise RuntimeError("boom")
            return self.inner.execute(sql, *args, **kwargs)

        def register(self, *args, **kwargs):
            return self.inner.register(*args, **kwargs)

        def close(self):
            events.append("close")
            return self.inner.close()

    def _connect(path):
        return _ConnectionProxy(real_connect(path))

    monkeypatch.setattr(duckdb_helpers.duckdb, "connect", _connect)

    replace_rows_by_file(
        tmp_path / "docs.duckdb",
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1]}),
        file_hash="file-a",
    )

    with pytest.raises(RuntimeError, match="boom"):
        replace_rows_by_file(
            tmp_path / "docs.duckdb",
            "fact_claim",
            df=pl.DataFrame({"claim_id": [2], "group": ["docs"]}),
            file_hash="file-b",
        )

    assert events[-3:] == ["begin", "rollback", "close"]


def test_replace_rows_by_values_replaces_one_value_slice_without_touching_other_values(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame(
            {
                "claim_id": [1, 2, 3],
                "status": ["open", "open", "ready"],
                "amount": [10, 20, 30],
            }
        ),
        column="status",
    )
    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame(
            {
                "claim_id": [4],
                "status": ["done"],
                "amount": [40],
            }
        ),
        column="status",
    )
    returned = replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame(
            {
                "claim_id": [10, 11],
                "status": ["open", "open"],
                "amount": [100, 110],
            }
        ),
        column="status",
    )

    assert returned.to_dict(as_series=False) == {
        "claim_id": [10, 11],
        "status": ["open", "open"],
        "amount": [100, 110],
    }

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "status", "amount" FROM "fact_claim" ORDER BY "status", "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [4, 10, 11, 3],
        "status": ["done", "open", "open", "ready"],
        "amount": [40, 100, 110, 30],
    }


def test_replace_rows_by_values_expands_schema_and_allows_side_effect_only_calls(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "status": ["open"]}),
        column="status",
    )
    result = replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [2], "status": ["ready"], "group": ["docs"]}),
        column="status",
        return_df=False,
    )

    assert result is None

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "status", "group" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [1, 2],
        "status": ["open", "ready"],
        "group": [None, "docs"],
    }


def test_replace_rows_by_values_is_not_dependent_on_df_column_order(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "status": ["open"], "amount": [10]}),
        column="status",
    )
    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"amount": [20], "status": ["ready"], "claim_id": [2]}),
        column="status",
    )

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "status", "amount" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [1, 2],
        "status": ["open", "ready"],
        "amount": [10, 20],
    }


def test_replace_rows_by_values_replaces_null_value_slice(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "status": ["open"], "amount": [10]}),
        column="status",
    )
    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [2], "status": [None], "amount": [20]}),
        column="status",
    )
    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [3], "status": ["ready"], "amount": [30]}),
        column="status",
    )
    replace_rows_by_values(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [4], "status": [None], "amount": [40]}),
        column="status",
    )

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "status", "amount" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [1, 3, 4],
        "status": ["open", "ready", None],
        "amount": [10, 30, 40],
    }


def test_replace_rows_by_values_does_not_copy_incoming_to_temp_table(monkeypatch, tmp_path):
    real_connect = duckdb.connect

    class _ConnectionProxy:
        def __init__(self, inner):
            self.inner = inner

        def execute(self, sql: str, *args, **kwargs):
            statement = str(sql).strip().upper()
            assert "__DATA_ENGINE_REPLACE_VALUES_DF_TABLE" not in statement
            return self.inner.execute(sql, *args, **kwargs)

        def register(self, *args, **kwargs):
            return self.inner.register(*args, **kwargs)

        def close(self):
            return self.inner.close()

    def _connect(path):
        return _ConnectionProxy(real_connect(path))

    monkeypatch.setattr(duckdb_helpers.duckdb, "connect", _connect)

    replace_rows_by_values(
        tmp_path / "docs.duckdb",
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "status": ["open"]}),
        column="status",
    )


def test_replace_rows_by_values_rolls_back_and_closes_connection_on_failure(monkeypatch, tmp_path):
    real_connect = duckdb.connect
    events: list[str] = []

    class _ConnectionProxy:
        def __init__(self, inner):
            self.inner = inner

        def execute(self, sql: str, *args, **kwargs):
            statement = str(sql).strip().upper()
            if statement.startswith("BEGIN"):
                events.append("begin")
            elif statement.startswith("ROLLBACK"):
                events.append("rollback")
            elif statement.startswith("COMMIT"):
                events.append("commit")
            if "ALTER TABLE" in statement and '"GROUP"' in statement:
                raise RuntimeError("boom")
            return self.inner.execute(sql, *args, **kwargs)

        def register(self, *args, **kwargs):
            return self.inner.register(*args, **kwargs)

        def close(self):
            events.append("close")
            return self.inner.close()

    def _connect(path):
        return _ConnectionProxy(real_connect(path))

    monkeypatch.setattr(duckdb_helpers.duckdb, "connect", _connect)

    replace_rows_by_values(
        tmp_path / "docs.duckdb",
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "status": ["open"]}),
        column="status",
    )

    with pytest.raises(RuntimeError, match="boom"):
        replace_rows_by_values(
            tmp_path / "docs.duckdb",
            "fact_claim",
            df=pl.DataFrame({"claim_id": [2], "status": ["ready"], "group": ["docs"]}),
            column="status",
        )

    assert events[-3:] == ["begin", "rollback", "close"]


def test_attach_dimension_joins_existing_dimension_without_dropping_key_columns_by_default(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    df = pl.DataFrame(
        {
            "member_id": ["a", "a", "b"],
            "lob": ["medical", "medical", "dental"],
            "amount": [10, 20, 30],
        }
    )
    build_dimension(
        db_path,
        "dim_member",
        df=df.select(["member_id", "lob"]).unique(maintain_order=True),
        key_column="member_key",
    )

    attached = attach_dimension(
        db_path,
        "dim_member",
        df=df,
        on=["member_id", "lob"],
        key_column="member_key",
    )

    assert attached.to_dict(as_series=False) == {
        "member_id": ["a", "a", "b"],
        "lob": ["medical", "medical", "dental"],
        "amount": [10, 20, 30],
        "member_key": [1, 1, 2],
    }


def test_normalize_columns_can_return_mapping_only(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    df = pl.DataFrame(
        {
            "member_id": ["a", "a", "b"],
            "lob": ["medical", "medical", "dental"],
            "amount": [10, 20, 30],
        }
    )

    mapping = normalize_columns(
        db_path,
        "dim_member",
        df=df,
        on=["member_id", "lob"],
        key_column="member_key",
        returns="map",
    )

    assert mapping.to_dict(as_series=False) == {
        "member_id": ["a", "b"],
        "lob": ["medical", "dental"],
        "member_key": [1, 2],
    }


def test_attach_dimension_can_drop_key_columns_when_requested(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    df = pl.DataFrame({"status": ["open", "ready", "open"], "amount": [10, 20, 30]})
    build_dimension(
        db_path,
        "dim_status",
        df=df.select(["status"]).unique(maintain_order=True),
        key_column="status_key",
    )

    attached = attach_dimension(
        db_path,
        "dim_status",
        df=df,
        on="status",
        key_column="status_key",
        drop_key=True,
    )

    assert attached.to_dict(as_series=False) == {
        "amount": [10, 20, 30],
        "status_key": [1, 2, 1],
    }


def test_normalize_columns_returns_normalized_frame_for_composite_keys_by_default(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    df = pl.DataFrame(
        {
            "member_id": ["a", "a", "b"],
            "lob": ["medical", "medical", "dental"],
            "amount": [10, 20, 30],
        }
    )

    normalized = normalize_columns(
        db_path,
        "dim_member",
        df=df,
        on=["member_id", "lob"],
        key_column="member_key",
    )

    assert normalized.to_dict(as_series=False) == {
        "amount": [10, 20, 30],
        "member_key": [1, 1, 2],
    }


def test_normalize_columns_collects_lazy_frames(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    lazy_df = pl.DataFrame({"status": ["open", "ready", "open"], "amount": [10, 20, 30]}).lazy()

    normalized = normalize_columns(
        db_path,
        "dim_status",
        df=lazy_df,
        on="status",
        key_column="status_key",
    )

    assert normalized.to_dict(as_series=False) == {
        "amount": [10, 20, 30],
        "status_key": [1, 2, 1],
    }


def test_read_rows_by_values_returns_selected_columns_for_matching_rows(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame(
            {
                "claim_id": [1, 2, 3],
                "status": ["open", "ready", "done"],
                "amount": [10, 20, 30],
            }
        ),
        file_hash="file-a",
    )

    result = read_rows_by_values(
        db_path,
        "fact_claim",
        column="claim_id",
        is_in=[1, 3],
        select=["claim_id", "amount"],
    )

    assert result.to_dict(as_series=False) == {
        "claim_id": [1, 3],
        "amount": [10, 30],
    }


def test_read_rows_by_values_quotes_reserved_identifiers_and_supports_single_selected_column(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "mart.fact_group",
        df=pl.DataFrame(
            {
                "group": ["docs", "eligibility"],
                "status": ["open", "ready"],
            }
        ),
        file_hash="file-a",
    )

    result = read_rows_by_values(
        db_path,
        "mart.fact_group",
        column="group",
        is_in=["docs"],
        select="status",
    )

    assert result.to_dict(as_series=False) == {"status": ["open"]}


def test_read_rows_by_values_returns_empty_frame_when_values_list_is_empty(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )

    result = read_rows_by_values(
        db_path,
        "fact_claim",
        column="claim_id",
        is_in=[],
        select=["claim_id", "amount"],
    )

    assert result.is_empty()
    assert result.columns == ["claim_id", "amount"]


def test_read_rows_by_values_supports_nulls_without_losing_lookup_order(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame(
            {
                "claim_id": [1, 2, 3],
                "status": ["open", None, "ready"],
                "amount": [10, 20, 30],
            }
        ),
        file_hash="file-a",
    )

    result = read_rows_by_values(
        db_path,
        "fact_claim",
        column="status",
        is_in=[None, "ready"],
        select=["status", "amount"],
    )

    assert result.to_dict(as_series=False) == {
        "status": [None, "ready"],
        "amount": [20, 30],
    }


def test_read_sql_returns_query_result_as_polars_dataframe(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1, 2], "amount": [10, 20]}),
        file_hash="file-a",
    )

    result = read_sql(
        db_path,
        sql='SELECT "claim_id", "amount" FROM "fact_claim" WHERE "amount" >= 20 ORDER BY "claim_id"',
    )

    assert result.to_dict(as_series=False) == {
        "claim_id": [2],
        "amount": [20],
    }


def test_read_table_supports_select_where_and_limit(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame(
            {
                "claim_id": [1, 2, 3],
                "status": ["open", "ready", "done"],
                "amount": [10, 20, 30],
            }
        ),
        file_hash="file-a",
    )

    result = read_table(
        db_path,
        "fact_claim",
        select=["claim_id", "amount"],
        where='"amount" >= 20',
        limit=1,
    )

    assert result.to_dict(as_series=False) == {
        "claim_id": [2],
        "amount": [20],
    }


def test_compact_database_drops_all_null_columns_and_can_target_specific_tables(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    with duckdb.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE fact_claim AS
            SELECT *
            FROM (
                VALUES
                    (1, 'open', NULL, NULL),
                    (2, 'ready', NULL, NULL)
            ) AS t(claim_id, status, empty_text, empty_number)
            """
        )
        connection.execute(
            """
            CREATE TABLE fact_other AS
            SELECT *
            FROM (
                VALUES
                    (10, NULL),
                    (11, NULL)
            ) AS t(other_id, notes)
            """
        )

    size_before = db_path.stat().st_size
    summary = compact_database(db_path, tables="fact_claim", vacuum=False)
    size_after = db_path.stat().st_size

    assert summary.to_dict(as_series=False) == {
        "db_path": [str(db_path.resolve())],
        "table": ["fact_claim"],
        "dropped_column_count": [2],
        "dropped_columns": [["empty_text", "empty_number"]],
        "vacuum_requested": [False],
        "vacuumed": [False],
        "size_before_bytes": [size_before],
        "size_after_bytes": [size_after],
    }

    with duckdb.connect(db_path) as connection:
        fact_claim = connection.execute("PRAGMA table_info('fact_claim')").fetchall()
        fact_other = connection.execute("PRAGMA table_info('fact_other')").fetchall()

    assert [row[1] for row in fact_claim] == ["claim_id", "status"]
    assert [row[1] for row in fact_other] == ["other_id", "notes"]


def test_compact_database_preserves_at_least_one_all_null_column_and_reports_vacuum(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    with duckdb.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE fact_empty AS
            SELECT *
            FROM (
                VALUES
                    (NULL, NULL),
                    (NULL, NULL)
            ) AS t(empty_a, empty_b)
            """
        )

    summary = compact_database(db_path)

    assert summary.get_column("table").to_list() == ["fact_empty"]
    assert summary.get_column("dropped_column_count").to_list() == [1]
    assert summary.get_column("dropped_columns").to_list() == [["empty_b"]]
    assert summary.get_column("vacuumed").to_list() == [True]

    with duckdb.connect(db_path) as connection:
        fact_empty = connection.execute("PRAGMA table_info('fact_empty')").fetchall()

    assert [row[1] for row in fact_empty] == ["empty_a"]


def test_compact_database_rejects_missing_tables(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    with duckdb.connect(db_path) as connection:
        connection.execute("CREATE TABLE fact_claim AS SELECT 1 AS claim_id")

    with pytest.raises(ValueError, match="must exist in database"):
        compact_database(db_path, tables=["fact_claim", "missing_table"], vacuum=False)


def test_ensure_index_creates_stable_index_for_columns(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )

    index_name = ensure_index(db_path, "fact_claim", columns="file_key")
    second_index_name = ensure_index(db_path, "fact_claim", columns="file_key")

    assert second_index_name == index_name
    with duckdb.connect(db_path) as connection:
        indexes = connection.execute(
            """
            SELECT index_name, table_name, expressions
            FROM duckdb_indexes()
            WHERE table_name = 'fact_claim'
            """
        ).fetchall()

    assert indexes == [(index_name, "fact_claim", "[file_key]")]


def test_ensure_index_accepts_custom_name_and_composite_columns(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "status": ["open"], "amount": [10]}),
        file_hash="file-a",
    )

    index_name = ensure_index(
        db_path,
        "fact_claim",
        columns=["status", "claim_id"],
        name="idx_fact_claim_status_claim",
    )

    assert index_name == "idx_fact_claim_status_claim"
    with duckdb.connect(db_path) as connection:
        indexes = connection.execute(
            """
            SELECT index_name, expressions
            FROM duckdb_indexes()
            WHERE index_name = 'idx_fact_claim_status_claim'
            """
        ).fetchall()

    assert indexes == [("idx_fact_claim_status_claim", "[status, claim_id]")]


def test_ensure_index_supports_schema_qualified_tables(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "mart.fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )

    index_name = ensure_index(db_path, "mart.fact_claim", columns="file_key")

    with duckdb.connect(db_path) as connection:
        indexes = connection.execute(
            """
            SELECT schema_name, index_name, table_name
            FROM duckdb_indexes()
            WHERE table_name = 'fact_claim'
            """
        ).fetchall()

    assert indexes == [("mart", index_name, "fact_claim")]


def test_ensure_index_rejects_missing_tables_columns_and_empty_names(tmp_path):
    db_path = tmp_path / "docs.duckdb"

    with pytest.raises(ValueError, match="does not exist"):
        ensure_index(db_path, "fact_claim", columns="file_key")

    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )

    with pytest.raises(ValueError, match="columns must exist"):
        ensure_index(db_path, "fact_claim", columns="missing")

    with pytest.raises(ValueError, match="name must be non-empty"):
        ensure_index(db_path, "fact_claim", columns="file_key", name=" ")


def test_replace_table_replaces_existing_rows_and_can_return_df(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_rows_by_file(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
        file_hash="file-a",
    )

    returned = replace_table(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [10, 11], "amount": [100, 110]}),
    )

    assert returned.to_dict(as_series=False) == {
        "claim_id": [10, 11],
        "amount": [100, 110],
    }

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "amount" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == returned.to_dict(as_series=False)


def test_replace_table_expands_schema_and_allows_side_effect_only_calls(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    replace_table(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [1], "amount": [10]}),
    )

    result = replace_table(
        db_path,
        "fact_claim",
        df=pl.DataFrame({"claim_id": [2], "amount": [20], "group": ["docs"]}),
        return_df=False,
    )

    assert result is None

    with duckdb.connect(db_path) as connection:
        persisted = connection.execute(
            'SELECT "claim_id", "amount", "group" FROM "fact_claim" ORDER BY "claim_id"'
        ).pl()

    assert persisted.to_dict(as_series=False) == {
        "claim_id": [2],
        "amount": [20],
        "group": ["docs"],
    }


def test_replace_table_collects_lazy_frames(tmp_path):
    db_path = tmp_path / "docs.duckdb"
    lazy_df = pl.DataFrame({"claim_id": [1, 2], "amount": [10, 20]}).lazy()

    returned = replace_table(
        db_path,
        "fact_claim",
        df=lazy_df,
    )

    assert returned.to_dict(as_series=False) == {
        "claim_id": [1, 2],
        "amount": [10, 20],
    }


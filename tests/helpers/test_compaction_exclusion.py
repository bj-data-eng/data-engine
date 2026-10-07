from __future__ import annotations

import duckdb
import pytest
import polars as pl
import subprocess
from pathlib import Path

from data_engine.helpers.duckdb import compact_database
from data_engine.helpers.duckdb import _maintenance
from data_engine.ui.cli.app import main as cli_main


@pytest.mark.parametrize("read_only", [False, True])
def test_schema_compaction_refuses_open_connections_without_mutating_database(tmp_path, read_only):
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(database) as connection:
        connection.execute("CREATE TABLE claims(id INTEGER, notes VARCHAR, unused VARCHAR)")
        connection.execute("INSERT INTO claims VALUES (1, NULL, NULL)")
        connection.execute("CREATE INDEX claims_id ON claims(id)")
    with duckdb.connect(database, read_only=read_only) as other:
        with pytest.raises(duckdb.IOException):
            compact_database(database, vacuum=False)
        assert [row[1] for row in other.execute("PRAGMA table_info('claims')").fetchall()] == ["id", "notes", "unused"]
        assert other.execute("SELECT index_name FROM duckdb_indexes()").fetchall() == [("claims_id",)]
        if not read_only:
            other.execute("INSERT INTO claims VALUES (2, 'preserved', NULL)")
    summary = compact_database(database, vacuum=False)
    with duckdb.connect(database) as connection:
        columns = [row[1] for row in connection.execute("PRAGMA table_info('claims')").fetchall()]
        if read_only:
            assert columns == ["id"]
        else:
            assert columns == ["id", "notes"]
            assert connection.execute("SELECT notes FROM claims WHERE id=2").fetchone() == ("preserved",)
    assert summary["indexes_recreated"].to_list() == [["claims_id"]]


def test_frozen_compaction_uses_internal_worker_entrypoint(tmp_path, monkeypatch):
    monkeypatch.setattr(_maintenance.sys, "frozen", True, raising=False)
    requests = []

    def run(command, **kwargs):
        requests.append(command)
        pl.DataFrame({"table": ["claims"]}).write_ipc(command[-1])
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(_maintenance.subprocess, "run", run)
    result = compact_database(tmp_path / "warehouse.duckdb")
    assert requests[0][0] == _maintenance.sys.executable
    assert requests[0][1] == "--internal-duckdb-maintenance"
    assert result["table"].to_list() == ["claims"]


def test_frozen_cli_routes_maintenance_before_gui_or_workspace_initialization(tmp_path, monkeypatch):
    from data_engine.helpers.duckdb import _maintenance_worker

    monkeypatch.setattr(_maintenance.sys, "frozen", True, raising=False)
    calls = []
    monkeypatch.setattr(_maintenance_worker, "main", lambda request, output: calls.append((request, output)) or 7)
    output = tmp_path / "result.arrow"
    assert cli_main(["--internal-duckdb-maintenance", "{}", str(output)]) == 7
    assert calls == [("{}", Path(output))]

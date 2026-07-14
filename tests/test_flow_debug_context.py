from __future__ import annotations

import json
from pathlib import Path
import re

import polars as pl

from data_engine.core.primitives import FlowDebugContext
from data_engine.hosts.daemon.runtime_ledger import DaemonRuntimeCacheProxy
from data_engine.services.debug_artifacts import debug_artifacts_dir, list_debug_artifacts
from data_engine.services.runtime_io import RuntimeIoLayer


def test_flow_debug_context_save_frame_writes_parquet_and_linked_metadata(tmp_path: Path) -> None:
    context = FlowDebugContext(
        root=tmp_path,
        workspace_id="docs2",
        flow_name="example_mirror",
        run_id="run-1",
        source_path="C:/input/docs_flat_1.xlsx",
        step_name="Read Excel",
    )

    artifact_path = context.save_frame(
        pl.DataFrame({"claim_id": [1], "status": ["OPEN"]}),
        name="docs_snapshot",
        info={"rows": 1},
    )

    assert artifact_path.suffix == ".parquet"
    assert artifact_path.exists()
    metadata_path = artifact_path.with_suffix(".json")
    assert metadata_path.exists()
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload["debug"]["flow_name"] == "example_mirror"
    assert payload["debug"]["step_name"] == "Read Excel"
    assert payload["debug"]["artifact_kind"] == "dataframe"
    assert payload["info"]["rows"] == 1
    assert re.search(
        r"example_mirror__Read-Excel__\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d+(?:[+-]\d{2}-\d{2}|Z)?__docs_snapshot\.parquet$",
        artifact_path.name,
    )
    assert payload["debug"]["display_name"].startswith("example_mirror / Read Excel / 20")


def test_flow_debug_context_save_json_writes_embedded_debug_payload(tmp_path: Path) -> None:
    context = FlowDebugContext(
        root=debug_artifacts_dir(tmp_path),
        workspace_id="docs2",
        flow_name="example_mirror",
        run_id="run-2",
        source_path="C:/input/docs_flat_2.xlsx",
        step_name="Write Summary",
    )

    artifact_path = context.save_json({"status": "ok"}, name="summary", info={"stage": "final"})

    assert artifact_path.suffix == ".json"
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["debug"]["artifact_kind"] == "json"
    assert payload["debug"]["step_name"] == "Write Summary"
    assert payload["data"]["status"] == "ok"
    assert payload["info"]["stage"] == "final"

    records = list_debug_artifacts(tmp_path)

    assert len(records) == 1
    record = records[0]
    assert record.kind == "json"
    assert record.artifact_path == artifact_path
    assert record.metadata_path == artifact_path
    assert record.flow_name == "example_mirror"
    assert record.step_name == "Write Summary"
    assert record.source_path == "C:/input/docs_flat_2.xlsx"
    assert record.metadata == payload


def test_list_debug_artifacts_ignores_sidecars_malformed_json_and_unrelated_json(tmp_path: Path) -> None:
    context = FlowDebugContext(
        root=debug_artifacts_dir(tmp_path),
        workspace_id="docs2",
        flow_name="example_mirror",
        run_id="run-3",
        source_path=None,
        step_name="Read Excel",
    )
    artifact_path = context.save_frame(pl.DataFrame({"claim_id": [1]}), name="rows")
    root = debug_artifacts_dir(tmp_path)
    (root / "notes.json").write_text('{"status": "unrelated"}', encoding="utf-8")
    (root / "example__Step__2026-04-19T00-00-00Z__malformed.json").write_text("{", encoding="utf-8")
    (root / "example__Step__2026-04-19T00-00-01Z__metadata.json").write_text(
        json.dumps({"debug": {"artifact_kind": "dataframe"}}),
        encoding="utf-8",
    )

    records = list_debug_artifacts(tmp_path)

    assert len(records) == 1
    assert records[0].artifact_path == artifact_path
    assert records[0].kind == "parquet"


def test_daemon_runtime_cache_proxy_preserves_runtime_db_path(tmp_path: Path) -> None:
    runtime_db_path = tmp_path / "runtime.db"
    runtime_store = RuntimeIoLayer(cache_ttl_seconds=0.0).open_cache_store(runtime_db_path)
    proxy = DaemonRuntimeCacheProxy(runtime_store, publish_event=lambda event_type, *, payload=None: None)
    try:
        assert proxy.db_path == runtime_db_path.resolve()
    finally:
        runtime_store.close()

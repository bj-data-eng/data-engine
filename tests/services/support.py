from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns

from data_engine.domain import FlowCatalogEntry
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_cache_store import RuntimeCacheLedger
from data_engine.runtime.runtime_db import utcnow_text


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def rewrite_with_new_timestamp(path: Path, contents: str, *, step_ns: int = 2_000_000_000) -> None:
    path.write_text(contents, encoding="utf-8")
    current = path.stat().st_mtime_ns
    bumped = max(current + step_ns, time_ns() + step_ns)
    os.utime(path, ns=(bumped, bumped))


def record_run_with_step(
    ledger: RuntimeCacheLedger,
    *,
    run_id: str,
    flow_name: str,
    step_label: str,
    status: str,
    output_path: str | None = None,
    error_text: str | None = None,
) -> int:
    started_at = datetime.now(UTC).isoformat()
    ledger.runs.record_started(
        run_id=run_id,
        flow_name=flow_name,
        group_name="Claims",
        source_path=None,
        started_at_utc=started_at,
    )
    step_run_id = ledger.step_outputs.record_started(
        run_id=run_id,
        flow_name=flow_name,
        step_label=step_label,
        started_at_utc=started_at,
    )
    ledger.step_outputs.record_finished(
        step_run_id=step_run_id,
        status=status,
        finished_at_utc=datetime.now(UTC).isoformat(),
        elapsed_ms=10,
        error_text=error_text,
        output_path=output_path,
    )
    ledger.runs.record_finished(
        run_id=run_id,
        status=status,
        finished_at_utc=datetime.now(UTC).isoformat(),
        error_text=error_text,
    )
    return step_run_id


def record_flow_state(ledger: RuntimeCacheLedger, *, flow_name: str, source_path: Path, run_id: str) -> None:
    started_at = utcnow_text()
    ledger.runs.record_started(
        run_id=run_id,
        flow_name=flow_name,
        group_name="Tests",
        source_path=str(source_path),
        started_at_utc=started_at,
    )
    step_run_id = ledger.step_outputs.record_started(
        run_id=run_id,
        flow_name=flow_name,
        step_label="Read",
        started_at_utc=started_at,
    )
    ledger.step_outputs.record_finished(
        step_run_id=step_run_id,
        status="success",
        finished_at_utc=started_at,
        elapsed_ms=5,
        output_path=str(source_path.with_suffix(".parquet")),
    )
    ledger.logs.append(
        level="INFO",
        message=f"run={run_id} flow={flow_name} source={source_path} status=success",
        created_at_utc=started_at,
        run_id=run_id,
        flow_name=flow_name,
    )
    signature = ledger.source_signatures.signature_for_path(source_path)
    assert signature is not None
    ledger.source_signatures.upsert_file_state(
        flow_name=flow_name,
        signature=signature,
        status="success",
        run_id=run_id,
        finished_at_utc=started_at,
    )
    ledger.runs.record_finished(
        run_id=run_id,
        status="success",
        finished_at_utc=started_at,
    )


def claims_poll_card() -> FlowCatalogEntry:
    return FlowCatalogEntry(
        name="claims",
        group="Claims",
        title="Claims",
        description="",
        source_root="(not set)",
        target_root="(not set)",
        mode="manual",
        interval="-",
        settle="-",
        operations="Write Output",
        operation_items=("Write Output",),
        state="manual",
        valid=True,
        category="manual",
    )

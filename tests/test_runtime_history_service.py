from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from data_engine.domain import FlowCatalogEntry, FlowLogEntry, FlowRunState, RuntimeStepEvent
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.services.runtime_history import RuntimeHistoryService


def _record_run_with_step(
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


def test_runtime_history_service_rebuilds_latest_existing_step_outputs(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    ledger = RuntimeCacheLedger.open_default(data_root=workspace_root)
    service = RuntimeHistoryService()
    first_output = tmp_path / "old.parquet"
    first_output.write_text("old", encoding="utf-8")
    second_output = tmp_path / "new.parquet"
    second_output.write_text("new", encoding="utf-8")

    try:
        _record_run_with_step(
            ledger,
            run_id="run-old",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(first_output),
        )
        _record_run_with_step(
            ledger,
            run_id="run-new",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(second_output),
        )
        _record_run_with_step(
            ledger,
            run_id="run-missing",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(tmp_path / "missing.parquet"),
        )

        flow_cards = {
            "claims": FlowCatalogEntry(
                name="claims",
                group="Claims",
                title="Claims",
                description="",
                source_root="(not set)",
                target_root="(not set)",
                mode="manual",
                interval="-",
                operations="Write Output",
                operation_items=("Write Output",),
                state="manual",
                valid=True,
                category="manual",
            )
        }

        rebuilt = service.rebuild_step_outputs(ledger, flow_cards)

        assert rebuilt.index.output_path("claims", "Write Output") == second_output
        assert rebuilt.last_step_run_id is not None
    finally:
        ledger.close()


def test_runtime_history_service_refreshes_step_outputs_incrementally(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    ledger = RuntimeCacheLedger.open_default(data_root=workspace_root)
    service = RuntimeHistoryService()
    first_output = tmp_path / "first.parquet"
    first_output.write_text("first", encoding="utf-8")
    second_output = tmp_path / "second.parquet"
    second_output.write_text("second", encoding="utf-8")
    flow_cards = {
        "claims": FlowCatalogEntry(
            name="claims",
            group="Claims",
            title="Claims",
            description="",
            source_root="(not set)",
            target_root="(not set)",
            mode="manual",
            interval="-",
            operations="Write Output",
            operation_items=("Write Output",),
            state="manual",
            valid=True,
            category="manual",
        )
    }

    try:
        _record_run_with_step(
            ledger,
            run_id="run-first",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(first_output),
        )
        initial = service.rebuild_step_outputs(ledger, flow_cards)

        _record_run_with_step(
            ledger,
            run_id="run-second",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(second_output),
        )
        refreshed = service.refresh_step_outputs(
            ledger,
            flow_cards,
            current_index=initial.index,
            last_seen_step_run_id=initial.last_step_run_id,
        )

        assert refreshed.index.output_path("claims", "Write Output") == second_output
        assert refreshed.last_step_run_id is not None
        assert initial.last_step_run_id is not None
        assert refreshed.last_step_run_id > initial.last_step_run_id
    finally:
        ledger.close()


def test_runtime_history_service_returns_step_error_details_before_run_error(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    ledger = RuntimeCacheLedger.open_default(data_root=workspace_root)
    service = RuntimeHistoryService()
    run_id = "run-step-error"
    _record_run_with_step(
        ledger,
        run_id=run_id,
        flow_name="claims",
        step_label="Transform Claims",
        status="failed",
        error_text="step blew up",
    )
    run_group = FlowRunState(
        key=("claims", run_id),
        display_label="Claims",
        source_label="-",
        status="failed",
        elapsed_seconds=None,
        summary_entry=None,
        steps=(),
        entries=(),
    )
    entry = FlowLogEntry(
        line="failed",
        kind="flow",
        flow_name="claims",
        event=RuntimeStepEvent(
            run_id=run_id,
            flow_name="claims",
            step_name="Transform Claims",
            source_label="-",
            status="failed",
        ),
    )

    try:
        assert service.error_text_for_entry(ledger, run_group, entry) == ("Transform Claims Error", "step blew up")
    finally:
        ledger.close()


def test_runtime_history_service_falls_back_to_run_error_details(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    ledger = RuntimeCacheLedger.open_default(data_root=workspace_root)
    service = RuntimeHistoryService()
    run_id = "run-error"
    _record_run_with_step(
        ledger,
        run_id=run_id,
        flow_name="claims",
        step_label="Transform Claims",
        status="failed",
        error_text="run level detail",
    )
    run_group = FlowRunState(
        key=("claims", run_id),
        display_label="Claims",
        source_label="-",
        status="failed",
        elapsed_seconds=None,
        summary_entry=None,
        steps=(),
        entries=(),
    )
    entry = FlowLogEntry(
        line="failed",
        kind="flow",
        flow_name="claims",
        event=RuntimeStepEvent(
            run_id=run_id,
            flow_name="claims",
            step_name="Different Step",
            source_label="-",
            status="failed",
        ),
    )

    try:
        assert service.error_text_for_entry(ledger, run_group, entry) == ("Run Error", "run level detail")
    finally:
        ledger.close()

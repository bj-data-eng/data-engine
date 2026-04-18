from __future__ import annotations

from pathlib import Path

from data_engine.domain import FlowLogEntry, FlowRunState, RuntimeStepEvent
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.services.runtime_history import RuntimeHistoryService

from tests.services.support import claims_poll_card, record_run_with_step


def test_runtime_history_service_rebuilds_latest_existing_step_outputs(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    ledger = RuntimeCacheLedger.open_default(data_root=workspace_root)
    service = RuntimeHistoryService()
    first_output = tmp_path / "old.parquet"
    first_output.write_text("old", encoding="utf-8")
    second_output = tmp_path / "new.parquet"
    second_output.write_text("new", encoding="utf-8")

    try:
        record_run_with_step(
            ledger,
            run_id="run-old",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(first_output),
        )
        record_run_with_step(
            ledger,
            run_id="run-new",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(second_output),
        )
        record_run_with_step(
            ledger,
            run_id="run-missing",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(tmp_path / "missing.parquet"),
        )

        rebuilt = service.rebuild_step_outputs(ledger, {"claims": claims_poll_card()})

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
    flow_cards = {"claims": claims_poll_card()}

    try:
        record_run_with_step(
            ledger,
            run_id="run-first",
            flow_name="claims",
            step_label="Write Output",
            status="success",
            output_path=str(first_output),
        )
        initial = service.rebuild_step_outputs(ledger, flow_cards)

        record_run_with_step(
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
    record_run_with_step(
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
    record_run_with_step(
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

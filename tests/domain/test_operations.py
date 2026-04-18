from __future__ import annotations

from data_engine.domain import OperationSessionState, RuntimeStepEvent


def _event(*, status: str, step_name: str, elapsed_seconds: float | None = None) -> RuntimeStepEvent:
    return RuntimeStepEvent(
        run_id="run-1",
        flow_name="claims_summary",
        step_name=step_name,
        source_label="input.xlsx",
        status=status,
        elapsed_seconds=elapsed_seconds,
    )


def test_operation_session_state_tracks_running_and_completed_steps():
    session = OperationSessionState.empty().ensure_flow("claims_summary", ("Read", "Write"))
    session, flash_index = session.apply_event("claims_summary", ("Read", "Write"), _event(status="started", step_name="Read"), now=5.0)

    assert flash_index is None
    assert session.row_state("claims_summary", "Read") is not None
    assert session.row_state("claims_summary", "Read").status == "running"

    session, flash_index = session.apply_event(
        "claims_summary",
        ("Read", "Write"),
        _event(status="success", step_name="Read", elapsed_seconds=0.4),
        now=5.4,
    )

    assert flash_index == 0
    assert session.row_state("claims_summary", "Read").status == "success"
    assert session.duration_text("claims_summary", "Read", now=5.4, formatter=lambda seconds: f"{seconds:.1f}s") == "0.4s"


def test_operation_session_state_normalizes_completed_success_rows():
    session = OperationSessionState.empty().ensure_flow("claims_summary", ("Read",))
    session, _ = session.apply_event(
        "claims_summary",
        ("Read",),
        _event(status="success", step_name="Read", elapsed_seconds=0.25),
        now=9.0,
    )

    normalized = session.normalize_completed("claims_summary")

    assert normalized.row_state("claims_summary", "Read").status == "idle"
    assert normalized.row_state("claims_summary", "Read").elapsed_seconds == 0.25

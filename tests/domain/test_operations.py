from __future__ import annotations

from data_engine.domain import OperationSessionState, RuntimeStepEvent


def _event(*, status: str, step_name: str, elapsed_seconds: float | None = None) -> RuntimeStepEvent:
    return RuntimeStepEvent(
        run_id="run-1",
        flow_name="docs_summary",
        step_name=step_name,
        source_label="input.xlsx",
        status=status,
        elapsed_seconds=elapsed_seconds,
    )


def test_operation_session_state_tracks_running_and_completed_steps():
    session = OperationSessionState.empty().ensure_flow("docs_summary", ("Read", "Write"))
    session, flash_index = session.apply_event("docs_summary", ("Read", "Write"), _event(status="started", step_name="Read"), now=5.0)

    assert flash_index is None
    assert session.row_state("docs_summary", "Read") is not None
    assert session.row_state("docs_summary", "Read").status == "running"

    session, flash_index = session.apply_event(
        "docs_summary",
        ("Read", "Write"),
        _event(status="success", step_name="Read", elapsed_seconds=0.4),
        now=5.4,
    )

    assert flash_index == 0
    assert session.row_state("docs_summary", "Read").status == "success"
    assert session.duration_text("docs_summary", "Read", now=5.4, formatter=lambda seconds: f"{seconds:.1f}s") == "0.4s"


def test_operation_session_state_normalizes_completed_success_rows():
    session = OperationSessionState.empty().ensure_flow("docs_summary", ("Read",))
    session, _ = session.apply_event(
        "docs_summary",
        ("Read",),
        _event(status="success", step_name="Read", elapsed_seconds=0.25),
        now=9.0,
    )

    normalized = session.normalize_completed("docs_summary")

    assert normalized.row_state("docs_summary", "Read").status == "idle"
    assert normalized.row_state("docs_summary", "Read").elapsed_seconds == 0.25


def test_operation_session_state_clears_prior_success_when_next_step_starts():
    session = OperationSessionState.empty().ensure_flow("docs_summary", ("Read", "Write"))
    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Write"),
        _event(status="success", step_name="Read", elapsed_seconds=0.25),
        now=9.0,
    )

    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Write"),
        _event(status="started", step_name="Write"),
        now=9.1,
    )

    assert session.row_state("docs_summary", "Read").status == "idle"
    assert session.row_state("docs_summary", "Read").elapsed_seconds == 0.25
    assert session.row_state("docs_summary", "Write").status == "running"


def test_operation_session_state_preserves_older_completed_step_durations_across_multiple_starts():
    session = OperationSessionState.empty().ensure_flow("docs_summary", ("Read", "Normalize", "Write"))
    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Normalize", "Write"),
        _event(status="success", step_name="Read", elapsed_seconds=0.25),
        now=9.0,
    )
    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Normalize", "Write"),
        _event(status="started", step_name="Normalize"),
        now=9.1,
    )
    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Normalize", "Write"),
        _event(status="success", step_name="Normalize", elapsed_seconds=0.40),
        now=9.5,
    )
    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Normalize", "Write"),
        _event(status="started", step_name="Write"),
        now=9.6,
    )

    assert session.row_state("docs_summary", "Read").status == "idle"
    assert session.row_state("docs_summary", "Read").elapsed_seconds == 0.25
    assert session.row_state("docs_summary", "Normalize").status == "idle"
    assert session.row_state("docs_summary", "Normalize").elapsed_seconds == 0.40
    assert session.row_state("docs_summary", "Write").status == "running"


def test_operation_session_state_clears_prior_running_row_when_next_step_starts():
    session = OperationSessionState.empty().ensure_flow("docs_summary", ("Read", "Write"))
    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Write"),
        _event(status="started", step_name="Read"),
        now=9.0,
    )

    session, _ = session.apply_event(
        "docs_summary",
        ("Read", "Write"),
        _event(status="started", step_name="Write"),
        now=9.2,
    )

    assert session.row_state("docs_summary", "Read").status == "idle"
    assert session.row_state("docs_summary", "Read").elapsed_seconds is None
    assert session.row_state("docs_summary", "Write").status == "running"


def test_operation_session_state_tracks_stopped_step_duration():
    session = OperationSessionState.empty().ensure_flow("docs_summary", ("Read",))
    session, _ = session.apply_event(
        "docs_summary",
        ("Read",),
        _event(status="started", step_name="Read"),
        now=4.0,
    )

    session, flash_index = session.apply_event(
        "docs_summary",
        ("Read",),
        _event(status="stopped", step_name="Read", elapsed_seconds=0.8),
        now=4.8,
    )

    assert flash_index is None
    assert session.row_state("docs_summary", "Read").status == "stopped"
    assert session.duration_text("docs_summary", "Read", now=4.8, formatter=lambda seconds: f"{seconds:.1f}s") == "0.8s"


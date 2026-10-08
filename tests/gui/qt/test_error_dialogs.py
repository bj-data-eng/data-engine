from __future__ import annotations

import pytest
from PySide6.QtWidgets import QDialog, QLabel, QListWidget, QPushButton, QTextEdit, QWidget

from data_engine.core.model import FlowExecutionError
from data_engine.runtime.runtime_db import utcnow_text
from data_engine.services.logs import LogService
from data_engine.ui.gui.dialogs.messages import show_message_box
from tests.gui.qt.support import _attach_message_capture, _dispose_window, _make_window


def test_multiline_polars_error_dialog_is_plain_text_and_scrollable(qapp, monkeypatch):
    del qapp
    detail = 'ColumnNotFoundError: missing\n\nResolved plan:\n  ---> FAILED HERE <---'
    traceback = 'Traceback (most recent call last):\n  File "helpers.py", line 3, in transform\nColumnNotFoundError: missing'
    error = FlowExecutionError(flow_name="docs", phase="step", step_label="Transform", detail=f"{detail}\n\nPython traceback:\n{traceback}")
    captured = []

    def inspect_dialog(dialog):
        bodies = dialog.findChildren(QTextEdit)
        captured.extend(body.toPlainText() for body in bodies)
        assert all(body.isReadOnly() for body in bodies)
        return 0

    monkeypatch.setattr(QDialog, "exec", inspect_dialog)
    parent = QWidget()
    try:
        show_message_box(parent, title="Error", text=str(error), tone="error")
        assert captured == [detail, str(error)]
        captured.clear()
        show_message_box(parent, title="Error", text=traceback, tone="error")
        assert captured == [traceback]
    finally:
        parent.close()
        parent.deleteLater()


@pytest.mark.parametrize("with_step", [True, False], ids=["step-failure", "run-failure"])
def test_failed_run_inspect_works_without_log_messages(qapp, monkeypatch, with_step):
    window = _make_window()
    capture = _attach_message_capture(window)
    ledger = window.runtime_binding.runtime_cache_ledger
    created = utcnow_text()
    error_text = 'Flow "poller" failed in step "Transform": ColumnNotFoundError: missing\n\nResolved plan:\n  SELECT'
    ledger.runs.record_started(run_id="failed-helper", flow_name="poller", group_name="Tests", source_path=None, started_at_utc=created)
    if with_step:
        step_id = ledger.step_outputs.record_started(run_id="failed-helper", flow_name="poller", step_label="Transform", started_at_utc=created)
        ledger.step_outputs.record_finished(step_run_id=step_id, status="failed", finished_at_utc=created, elapsed_ms=1, error_text=error_text)
    ledger.runs.record_finished(run_id="failed-helper", status="failed", finished_at_utc=created, error_text=error_text)
    monkeypatch.setattr(ledger, "refresh_external_state", lambda: False)
    try:
        group, = window.history_query_service.list_flow_runs_from_ledger(ledger, flow_name="poller")
        assert group.entries == ()
        window._show_run_log_preview(group)
        dialog = window.run_log_preview_dialog
        assert dialog is not None
        log_list = dialog.findChild(QListWidget, "runLogList")
        assert log_list.count() == 1
        button, = log_list.findChildren(QPushButton, "inspectOutputButton")
        assert button.isEnabled()
        button.click()
        assert capture.shown_messages == [("Transform Error" if with_step else "Run Error", error_text, "error")]
    finally:
        _dispose_window(qapp, window)


def test_flow_level_failure_keeps_inspect_after_successful_step(qapp, monkeypatch):
    window = _make_window()
    capture = _attach_message_capture(window)
    ledger = window.runtime_binding.runtime_cache_ledger
    created = utcnow_text()
    ledger.runs.record_started(run_id="failed-between-steps", flow_name="poller", group_name="Tests", source_path=None, started_at_utc=created)
    step_id = ledger.step_outputs.record_started(run_id="failed-between-steps", flow_name="poller", step_label="Read", started_at_utc=created)
    ledger.step_outputs.record_finished(step_run_id=step_id, status="success", finished_at_utc=created, elapsed_ms=1)
    ledger.runs.record_finished(run_id="failed-between-steps", status="failed", finished_at_utc=created, error_text="missing object for next step")
    monkeypatch.setattr(ledger, "refresh_external_state", lambda: False)
    try:
        group, = window.history_query_service.list_flow_runs_from_ledger(ledger, flow_name="poller")
        window._show_run_log_preview(group)
        log_list = window.run_log_preview_dialog.findChild(QListWidget, "runLogList")
        assert log_list.count() == 2
        buttons = log_list.findChildren(QPushButton, "inspectOutputButton")
        enabled, = [button for button in buttons if button.isEnabled()]
        enabled.click()
        assert capture.shown_messages == [("Run Error", "missing object for next step", "error")]
    finally:
        _dispose_window(qapp, window)


@pytest.mark.parametrize("failure_position", ["step", "before-steps", "between-steps"])
@pytest.mark.parametrize("error_text", [
    "RuntimeError: intentional failure",
    'Flow "poller" failed in step "Transform" (function transform) for source "input/claims.xlsx": RuntimeError: intentional failure',
    'Flow "poller" failed in step "Transform": ColumnNotFoundError: missing\n\nResolved plan:\n  SELECT\n\nPython traceback:\n  File "helpers.py", line 3, in transform',
], ids=["plain-error", "structured-error", "query-plan-and-traceback"])
def test_run_log_keeps_failure_details_only_in_inspect(qapp, monkeypatch, failure_position, error_text):
    window = _make_window(log_service=LogService())
    capture = _attach_message_capture(window)
    ledger = window.runtime_binding.runtime_cache_ledger
    created = utcnow_text()
    run_id = "failed-with-diagnostics"
    ledger.runs.record_started(run_id=run_id, flow_name="poller", group_name="Tests", source_path="input/claims.xlsx", started_at_utc=created)
    if failure_position != "before-steps":
        step_id = ledger.step_outputs.record_started(run_id=run_id, flow_name="poller", step_label="Transform", started_at_utc=created)
        ledger.step_outputs.record_finished(
            step_run_id=step_id,
            status="failed" if failure_position == "step" else "success",
            finished_at_utc=created,
            elapsed_ms=1,
            error_text=error_text if failure_position == "step" else None,
        )
    ledger.logs.append(level="INFO", message="Read input successfully", created_at_utc=created, run_id=run_id, flow_name="poller")
    ledger.logs.append(level="WARNING", message="Continuing with fallback settings", created_at_utc=created, run_id=run_id, flow_name="poller")
    ledger.logs.append(level="ERROR", message="Retryable lookup error; using cached settings", created_at_utc=created, run_id=run_id, flow_name="poller")
    ledger.logs.append(level="ERROR", message=error_text, created_at_utc=created, run_id=run_id, flow_name="poller")
    ledger.runs.record_finished(run_id=run_id, status="failed", finished_at_utc=created, error_text=error_text)
    monkeypatch.setattr(ledger, "refresh_external_state", lambda: False)
    try:
        group, = window.history_query_service.list_flow_runs_from_ledger(ledger, flow_name="poller")
        window._show_run_log_preview(group)
        log_list = window.run_log_preview_dialog.findChild(QListWidget, "runLogList")
        assert log_list.count() == (5 if failure_position == "between-steps" else 4)
        messages = [label.text() for label in log_list.findChildren(QLabel, "rawLogMessage")]
        assert "Read input successfully" in messages
        assert "Continuing with fallback settings" in messages
        assert "Retryable lookup error; using cached settings" in messages
        assert all("RuntimeError" not in text and "ColumnNotFoundError" not in text and "traceback" not in text for text in messages)
        enabled, = [button for button in log_list.findChildren(QPushButton, "inspectOutputButton") if button.isEnabled()]
        enabled.click()
        assert capture.shown_messages == [("Transform Error" if failure_position == "step" else "Run Error", error_text, "error")]
        assert ledger.runs.get(run_id).error_text == error_text
        assert error_text in [entry.message for entry in ledger.logs.list(run_id=run_id)]
    finally:
        _dispose_window(qapp, window)

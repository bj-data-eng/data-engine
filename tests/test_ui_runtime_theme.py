from __future__ import annotations

import logging
from queue import Queue

import pytest

from data_engine.domain import FlowLogEntry, RuntimeStepEvent
from data_engine.platform.theme import GITHUB_DARK, GITHUB_LIGHT
from data_engine.ui.gui import runtime as gui_runtime
from data_engine.ui.gui import theme as gui_theme
from data_engine.ui.tui import runtime as tui_runtime
from data_engine.ui.tui import theme as tui_theme


class _FailingQueue:
    def put_nowait(self, item):  # noqa: ANN001
        raise RuntimeError("queue closed")


class _RecordingHandler(gui_runtime.QueueLogHandler):
    def __init__(self, queue):
        super().__init__(queue)
        self.handled = False

    def handleError(self, record):  # noqa: N802, ANN001
        self.handled = True


@pytest.mark.parametrize("handler_cls", [gui_runtime.QueueLogHandler, tui_runtime.QueueLogHandler])
def test_queue_log_handlers_emit_flow_and_system_entries(handler_cls):
    queue: Queue[FlowLogEntry] = Queue()
    handler = handler_cls(queue)

    flow_record = logging.makeLogRecord(
        {"msg": "run=abc flow=claims_poll step=Write Parquet source=/tmp/input.xlsx status=success elapsed=1.25"}
    )
    system_record = logging.makeLogRecord({"msg": "daemon started at /tmp/data_engine.log"})

    handler.emit(flow_record)
    handler.emit(system_record)

    flow_entry = queue.get_nowait()
    system_entry = queue.get_nowait()

    assert flow_entry.kind == "flow"
    assert flow_entry.flow_name == "claims_poll"
    assert flow_entry.line == "claims_poll  Write Parquet  success  input.xlsx"
    assert flow_entry.event == RuntimeStepEvent(
        run_id="abc",
        flow_name="claims_poll",
        step_name="Write Parquet",
        source_label="input.xlsx",
        status="success",
        elapsed_seconds=1.25,
    )

    assert system_entry.kind == "system"
    assert system_entry.flow_name is None
    assert system_entry.line == "daemon started at data_engine.log"
    assert system_entry.event is None


def test_queue_log_handler_calls_handle_error_when_queue_write_fails():
    handler = _RecordingHandler(_FailingQueue())
    record = logging.makeLogRecord({"msg": "run=abc flow=alpha source=/tmp/input.xlsx status=started"})

    handler.emit(record)

    assert handler.handled is True


def test_gui_and_tui_theme_stylesheets_embed_requested_palette_tokens():
    dark_gui = gui_theme.stylesheet("dark")
    light_gui = gui_theme.stylesheet("light")
    dark_tui = tui_theme.stylesheet("dark")
    light_tui = tui_theme.stylesheet("light")

    assert GITHUB_DARK.app_bg in dark_gui
    assert GITHUB_LIGHT.app_bg in light_gui
    assert GITHUB_DARK.panel_bg in dark_tui
    assert GITHUB_LIGHT.panel_bg in light_tui
    assert dark_gui != light_gui
    assert dark_tui != light_tui


def test_tui_theme_exports_the_generated_stylesheet_constant():
    assert isinstance(tui_theme.TUI_CSS, str)
    assert "#header" in tui_theme.TUI_CSS
    assert "#flow-list-pane" in tui_theme.TUI_CSS
    assert tui_theme.resolve_theme_name("dark") == "dark"

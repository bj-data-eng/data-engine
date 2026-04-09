"""Output inspection helper functions for the desktop GUI."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.views.state import artifact_key_for_operation as artifact_key_for_operation_helper
from data_engine.views.state import capture_step_outputs as capture_step_outputs_helper
from data_engine.views.state import is_inspectable_operation as is_inspectable_operation_helper
from data_engine.domain import ConfigPreviewState
from data_engine.ui.gui.dialogs import show_config_preview as show_config_preview_dialog
from data_engine.ui.gui.dialogs import show_output_preview as show_output_preview_dialog
from data_engine.ui.gui.preview_models import ConfigPreviewRequest, OutputPreviewRequest

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def is_inspectable_operation(operation_name: str) -> bool:
    return is_inspectable_operation_helper(operation_name)


def artifact_key_for_operation(operation_name: str) -> str | None:
    return artifact_key_for_operation_helper(operation_name)


def capture_step_outputs(window: "DataEngineWindow", flow_name: str, results: object) -> None:
    updated = capture_step_outputs_helper(
        window.flow_cards[flow_name],
        window.step_output_index.outputs_for(flow_name).outputs,
        results,
    )
    window.step_output_index = window.step_output_index.with_flow_outputs(flow_name, updated)


def rehydrate_step_outputs_from_ledger(window: "DataEngineWindow") -> None:
    window.step_output_index = window.runtime_history_service.rebuild_step_outputs(
        window.runtime_binding.runtime_ledger,
        window.flow_cards,
    )


def refresh_operation_buttons(window: "DataEngineWindow", flow_name: str) -> None:
    for row_widgets in window.operation_row_widgets:
        if row_widgets.inspect_button is None:
            continue
        row_widgets.inspect_button.setEnabled(window.step_output_index.has_output(flow_name, row_widgets.operation_name))


def inspect_step_output(window: "DataEngineWindow", operation_name: str) -> None:
    request = build_output_preview_request(window, operation_name)
    if request is None:
        window._show_message_box(
            title="Inspect Output",
            text="No output is available for this step yet.",
            tone="info",
        )
        return
    window._show_output_preview(request.operation_name, request.output_path)


def show_output_preview(window: "DataEngineWindow", operation_name: str, output_path: Path) -> None:
    request = OutputPreviewRequest(operation_name=operation_name, output_path=output_path)
    window.output_preview_dialog = show_output_preview_dialog(window, request)


def show_config_preview(window: "DataEngineWindow") -> None:
    card = window.flow_cards.get(window.selected_flow_name or "")
    preview_state = ConfigPreviewState.from_flow(card, window.flow_states)
    window.config_preview_dialog = show_config_preview_dialog(window, ConfigPreviewRequest(preview=preview_state))


def build_output_preview_request(window: "DataEngineWindow", operation_name: str) -> OutputPreviewRequest | None:
    """Build one explicit output-preview request for the selected flow."""
    if window.selected_flow_name is None:
        return None
    output_path = window.step_output_index.output_path(window.selected_flow_name, operation_name)
    if output_path is None or not output_path.exists():
        return None
    return OutputPreviewRequest(operation_name=operation_name, output_path=output_path)

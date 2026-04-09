"""Shared presentation models and helpers across Data Engine surfaces."""

from data_engine.domain import (
    FlowLogEntry,
    FlowRunState,
    LogKind,
    RunKey,
    RuntimeStepEvent,
    format_log_line,
    format_runtime_message,
    parse_runtime_event,
    parse_runtime_message,
    short_source_label,
)
from data_engine.views.logs import FlowLogStore
from data_engine.views.actions import GuiActionState, TuiActionState
from data_engine.views.artifacts import ArtifactPreviewSpec, classify_artifact_preview, is_text_artifact
from data_engine.views.flow_display import FlowRowDisplay, GroupRowDisplay
from data_engine.views.models import (
    QtFlowCard,
    default_flow_state,
    flow_category,
    load_qt_flow_cards,
    qt_flow_card_from_entry,
    qt_flow_cards_from_entries,
)
from data_engine.views.presentation import (
    flow_group_name,
    flow_secondary_text,
    format_seconds,
    group_cards,
    group_label,
    group_secondary_text,
    operation_marker,
    state_dot,
    status_color_name,
)
from data_engine.views.state import (
    OperationDisplayState,
    OperationRowState,
    artifact_key_for_operation,
    build_flow_summary,
    capture_step_outputs,
    is_inspectable_operation,
)
from data_engine.views.status import WORKSPACE_UNAVAILABLE_TEXT, surface_control_status_text
from data_engine.views.runs import RunGroupDisplay, format_raw_log_message
from data_engine.views.text import (
    format_optional_seconds,
    pad,
    render_operation_lines,
    render_run_group_lines,
    render_selected_flow_lines,
    run_group_row_text,
    short_datetime,
)

__all__ = [
    "FlowLogStore",
    "FlowRowDisplay",
    "FlowLogEntry",
    "FlowRunState",
    "GroupRowDisplay",
    "GuiActionState",
    "ArtifactPreviewSpec",
    "LogKind",
    "OperationDisplayState",
    "OperationRowState",
    "QtFlowCard",
    "RunGroupDisplay",
    "RunKey",
    "RuntimeStepEvent",
    "TuiActionState",
    "artifact_key_for_operation",
    "build_flow_summary",
    "capture_step_outputs",
    "classify_artifact_preview",
    "default_flow_state",
    "flow_category",
    "flow_group_name",
    "flow_secondary_text",
    "format_log_line",
    "format_raw_log_message",
    "format_runtime_message",
    "format_optional_seconds",
    "format_seconds",
    "group_cards",
    "group_label",
    "group_secondary_text",
    "is_text_artifact",
    "is_inspectable_operation",
    "load_qt_flow_cards",
    "operation_marker",
    "pad",
    "parse_runtime_event",
    "parse_runtime_message",
    "render_operation_lines",
    "render_run_group_lines",
    "render_selected_flow_lines",
    "run_group_row_text",
    "short_datetime",
    "short_source_label",
    "surface_control_status_text",
    "state_dot",
    "status_color_name",
    "WORKSPACE_UNAVAILABLE_TEXT",
    "qt_flow_card_from_entry",
    "qt_flow_cards_from_entries",
]

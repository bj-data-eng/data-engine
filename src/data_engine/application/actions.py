"""Host-agnostic operator action-state use cases."""

from __future__ import annotations

from data_engine.domain import OperatorActionContext, SelectedFlowState


class ActionStateApplication:
    """Own host-neutral action-context assembly for operator surfaces."""

    def build_action_context(
        self,
        *,
        card,
        flow_states: dict[str, str],
        runtime_session,
        flow_groups_by_name: dict[str, str | None],
        active_flow_states,
        has_logs: bool,
        has_automated_flows: bool,
        workspace_available: bool = True,
        selected_run_group_present: bool = False,
        local_request_pending: bool = False,
    ) -> OperatorActionContext:
        """Return one operator action context from current runtime and selection state."""
        selected_flow = SelectedFlowState.from_runtime(
            card=card,
            flow_states=flow_states,
            runtime_session=runtime_session,
            flow_groups_by_name=flow_groups_by_name,
            active_flow_states=active_flow_states,
            has_logs=has_logs,
        )
        return OperatorActionContext(
            runtime_session=runtime_session,
            selected_flow=selected_flow,
            has_automated_flows=has_automated_flows,
            workspace_available=workspace_available,
            selected_run_group_present=selected_run_group_present,
            local_request_pending=local_request_pending,
        )


__all__ = ["ActionStateApplication"]

"""Host-agnostic selected-flow and run-detail use cases."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.domain import FlowCatalogLike, FlowRunState, SelectedFlowDetailState


@dataclass(frozen=True)
class SelectedFlowPresentation:
    """Normalized selected-flow detail state for operator surfaces."""

    detail_state: SelectedFlowDetailState | None
    run_groups: tuple[FlowRunState, ...]
    visible_run_groups: tuple[FlowRunState, ...]
    selected_run_key: tuple[str, str] | None
    empty_text: str

    @property
    def run_group_signature(self) -> tuple[tuple[str, str], ...]:
        """Return the stable visible run-list signature for diffing/render reuse."""
        return tuple(group.key for group in self.visible_run_groups)

    @property
    def selected_run_group(self) -> FlowRunState | None:
        """Return the normalized selected run group, if any."""
        if self.selected_run_key is not None:
            for run_group in self.run_groups:
                if run_group.key == self.selected_run_key:
                    return run_group
        return self.run_groups[0] if self.run_groups else None


class DetailApplication:
    """Own host-neutral selected-flow detail and run selection behavior."""

    def build_selected_flow_presentation(
        self,
        *,
        card: FlowCatalogLike | None,
        tracker,
        flow_states: dict[str, str],
        run_groups: tuple[FlowRunState, ...],
        selected_run_key: tuple[str, str] | None,
        max_visible_runs: int | None = None,
    ) -> SelectedFlowPresentation:
        """Return the selected-flow detail state and normalized run selection."""
        if card is None:
            return SelectedFlowPresentation(
                detail_state=None,
                run_groups=(),
                visible_run_groups=(),
                selected_run_key=None,
                empty_text="Select one flow to see details.",
            )
        detail_state = SelectedFlowDetailState.from_flow(
            card,
            tracker,
            flow_states=flow_states,
        )
        normalized_key = selected_run_key if any(group.key == selected_run_key for group in run_groups) else (run_groups[0].key if run_groups else None)
        visible_run_groups = run_groups[-max_visible_runs:] if max_visible_runs is not None and max_visible_runs >= 0 else run_groups
        return SelectedFlowPresentation(
            detail_state=detail_state,
            run_groups=run_groups,
            visible_run_groups=visible_run_groups,
            selected_run_key=normalized_key,
            empty_text="",
        )


__all__ = ["DetailApplication", "SelectedFlowPresentation"]

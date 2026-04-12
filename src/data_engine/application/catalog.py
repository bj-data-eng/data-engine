"""Host-agnostic flow catalog use cases."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from data_engine.core.model import FlowValidationError
from data_engine.domain import FlowCatalogEntry, FlowCatalogLike, FlowCatalogState
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.services import FlowCatalogService
from data_engine.views.presentation import group_cards


@dataclass(frozen=True)
class FlowCatalogLoadResult:
    """Normalized result of one workspace catalog load attempt."""

    catalog_state: FlowCatalogState
    loaded: bool
    error_text: str | None = None


@dataclass(frozen=True)
class FlowCatalogPresentation:
    """Normalized grouped catalog presentation shared by operator surfaces."""

    entries: tuple[FlowCatalogEntry, ...]
    grouped_entries: tuple[tuple[str, tuple[FlowCatalogEntry, ...]], ...]
    selected_flow_name: str | None

    @property
    def entries_by_name(self) -> dict[str, FlowCatalogEntry]:
        """Return entries keyed by internal flow name."""
        return {entry.name: entry for entry in self.entries}

    @property
    def selected_entry(self) -> FlowCatalogEntry | None:
        """Return the normalized selected entry, if any."""
        if self.selected_flow_name is None:
            return None
        return self.entries_by_name.get(self.selected_flow_name)

    @property
    def cards(self) -> tuple[FlowCatalogLike, ...]:
        """Return catalog entries under the shared flow metadata protocol."""
        return self.entries

    @property
    def grouped_cards(self) -> tuple[tuple[str, tuple[FlowCatalogLike, ...]], ...]:
        """Return grouped entries under the shared flow metadata protocol."""
        return self.grouped_entries

    @property
    def selected_card(self) -> FlowCatalogLike | None:
        """Return the selected flow metadata under the shared flow protocol."""
        return self.selected_entry

    @property
    def selected_list_index(self) -> int | None:
        """Return the list index for the selected flow in a grouped header+item list."""
        if self.selected_flow_name is None:
            return None
        index = 0
        for _group_name, entries in self.grouped_entries:
            index += 1
            for entry in entries:
                if entry.name == self.selected_flow_name:
                    return index
                index += 1
        return None


class FlowCatalogApplication:
    """Own host-neutral flow catalog loading and state transitions."""

    def __init__(self, *, flow_catalog_service: FlowCatalogService) -> None:
        self.flow_catalog_service = flow_catalog_service

    def load_state(
        self,
        *,
        workspace_root: Path,
        current_state: FlowCatalogState | None = None,
    ) -> FlowCatalogState:
        """Load discovered entries and merge them into one catalog state."""
        base = current_state or FlowCatalogState.empty()
        entries = self.flow_catalog_service.load_entries(workspace_root=workspace_root)
        state = base.with_entries(entries).with_empty_message("")
        if base.selected_flow_name is not None and state.selected_flow_name == base.selected_flow_name:
            return state
        return state.with_selected_flow_name(_first_grouped_entry_name(state.entries))

    def empty_state(
        self,
        *,
        message: str = "",
        current_state: FlowCatalogState | None = None,
    ) -> FlowCatalogState:
        """Return an empty catalog state with one host-provided message."""
        base = current_state or FlowCatalogState.empty()
        return FlowCatalogState.empty(empty_message=message).with_selected_flow_name(base.selected_flow_name)

    def select_flow(
        self,
        *,
        catalog_state: FlowCatalogState,
        flow_name: str | None,
    ) -> FlowCatalogState:
        """Return catalog state with one normalized selected flow."""
        return catalog_state.with_selected_flow_name(flow_name)

    def build_presentation(
        self,
        *,
        catalog_state: FlowCatalogState,
    ) -> FlowCatalogPresentation:
        """Return grouped UI-friendly catalog presentation from one catalog state."""
        grouped = tuple(
            (bucket.group_name, bucket.entries)
            for bucket in group_cards(catalog_state.entries)
        )
        return FlowCatalogPresentation(
            entries=catalog_state.entries,
            grouped_entries=grouped,
            selected_flow_name=catalog_state.selected_flow_name,
        )

    def load_workspace_catalog(
        self,
        *,
        workspace_paths: WorkspacePaths,
        current_state: FlowCatalogState | None = None,
        missing_message: str = "No flow modules discovered.",
    ) -> FlowCatalogLoadResult:
        """Return one normalized catalog load result for a resolved workspace binding."""
        if not workspace_paths.flow_modules_dir.is_dir():
            return FlowCatalogLoadResult(
                catalog_state=self.empty_state(message=missing_message, current_state=current_state),
                loaded=False,
            )
        try:
            catalog_state = self.load_state(
                workspace_root=workspace_paths.workspace_root,
                current_state=current_state,
            )
        except FlowValidationError as exc:
            message = str(exc)
            return FlowCatalogLoadResult(
                catalog_state=self.empty_state(message=message, current_state=current_state),
                loaded=False,
                error_text=message,
            )
        return FlowCatalogLoadResult(catalog_state=catalog_state, loaded=True)


def _first_grouped_entry_name(entries: tuple[FlowCatalogEntry, ...]) -> str | None:
    """Return the first entry name in the same grouped order used by operator surfaces."""
    for bucket in group_cards(entries):
        if bucket.entries:
            return bucket.entries[0].name
    return None

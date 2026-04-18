from __future__ import annotations

from pathlib import Path

from data_engine.application import FlowCatalogApplication
from data_engine.core.model import FlowValidationError
from data_engine.domain import FlowCatalogEntry
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


class _FakeFlowCatalogService:
    def __init__(self, entries: tuple[FlowCatalogEntry, ...]) -> None:
        self.entries = entries
        self.calls: list[Path] = []

    def load_entries(self, *, workspace_root: Path | None = None) -> tuple[FlowCatalogEntry, ...]:
        assert workspace_root is not None
        self.calls.append(workspace_root)
        return self.entries


class _FailingFlowCatalogService:
    def load_entries(self, *, workspace_root: Path | None = None) -> tuple[FlowCatalogEntry, ...]:
        assert workspace_root is not None
        raise FlowValidationError("invalid flow")


def _entry(
    *,
    name: str,
    group: str | None = "Examples",
    title: str | None = None,
    mode: str = "manual",
    interval: str = "-",
    operations: str = "Extract -> Write",
    operation_items: tuple[str, ...] = ("Extract", "Write"),
    state: str = "manual",
    category: str = "manual",
) -> FlowCatalogEntry:
    return FlowCatalogEntry(
        name=name,
        group=group,
        title=title or name.replace("_", " ").title(),
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode=mode,
        interval=interval,
        operations=operations,
        operation_items=operation_items,
        state=state,
        valid=True,
        category=category,
    )


def test_flow_catalog_application_loads_state(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    entry = _entry(name="example_manual", title="Example Manual")
    service = _FakeFlowCatalogService((entry,))

    state = FlowCatalogApplication(flow_catalog_service=service).load_state(workspace_root=workspace_root)

    assert service.calls == [workspace_root]
    assert state.entries == (entry,)
    assert state.selected_flow_name == "example_manual"
    assert state.empty_message == ""


def test_flow_catalog_application_reports_missing_workspace_catalog(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspaces" / "alpha"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    result = FlowCatalogApplication(flow_catalog_service=_FakeFlowCatalogService(())).load_workspace_catalog(
        workspace_paths=paths,
        missing_message="No flow modules discovered.",
    )

    assert result.loaded is False
    assert result.error_text is None
    assert result.catalog_state.empty_message == "No flow modules discovered."


def test_flow_catalog_application_reports_validation_errors(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspaces" / "alpha"
    (workspace_root / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    result = FlowCatalogApplication(flow_catalog_service=_FailingFlowCatalogService()).load_workspace_catalog(
        workspace_paths=paths,
    )

    assert result.loaded is False
    assert result.error_text == "invalid flow"
    assert result.catalog_state.empty_message == "invalid flow"


def test_flow_catalog_application_builds_grouped_presentation_and_selected_index(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    entries = (
        _entry(
            name="manual_review",
            group="Manual",
            title="Manual Review",
            operations="Review",
            operation_items=("Review",),
        ),
        _entry(
            name="poller",
            group="Imports",
            title="Poller",
            mode="poll",
            interval="30s",
            operations="Read",
            operation_items=("Read",),
            state="poll ready",
            category="automated",
        ),
    )
    app = FlowCatalogApplication(flow_catalog_service=_FakeFlowCatalogService(entries))
    state = app.load_state(workspace_root=workspace_root).with_selected_flow_name("poller")

    presentation = app.build_presentation(catalog_state=state)

    assert tuple(group_name for group_name, _entries in presentation.grouped_cards) == ("Imports", "Manual")
    assert presentation.selected_card is not None
    assert presentation.selected_card.name == "poller"
    assert presentation.selected_list_index == 1


def test_flow_catalog_application_selects_first_visual_flow_on_load(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    scheduled = _entry(
        name="scheduled_import",
        group=None,
        title="Scheduled Import",
        mode="schedule",
        interval="30s",
        operations="Read",
        operation_items=("Read",),
        state="schedule ready",
        category="automated",
    )
    manual = _entry(
        name="manual_review",
        group=None,
        title="Manual Review",
        operations="Review",
        operation_items=("Review",),
    )
    app = FlowCatalogApplication(flow_catalog_service=_FakeFlowCatalogService((scheduled, manual)))

    state = app.load_state(workspace_root=workspace_root)
    presentation = app.build_presentation(catalog_state=state)

    assert tuple(group_name for group_name, _entries in presentation.grouped_cards) == ("manual", "schedule")
    assert state.selected_flow_name == "manual_review"
    assert presentation.selected_list_index == 1

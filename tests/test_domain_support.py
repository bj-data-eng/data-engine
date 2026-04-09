from __future__ import annotations

from pathlib import Path

from data_engine.domain import DocumentationSessionState, WorkspaceSupportState


def test_documentation_session_state_tracks_root_and_build_state():
    docs = DocumentationSessionState.empty().with_build_running(True).with_root_dir(Path("/tmp/docs"))

    assert docs.build_running is True
    assert docs.available is True
    assert docs.root_dir == Path("/tmp/docs")


def test_workspace_support_state_updates_nested_docs_state():
    support = WorkspaceSupportState.empty()
    support = support.with_documentation(support.documentation.with_build_running(True))

    assert support.documentation.build_running is True

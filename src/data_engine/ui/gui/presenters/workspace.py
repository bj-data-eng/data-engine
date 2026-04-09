"""Compatibility re-exports for the split workspace presenter helpers."""

from data_engine.ui.gui.presenters.docs import (
    create_docs_browser,
    docs_build_dir,
    finish_docs_build,
    initialize_docs_view,
    load_docs_page,
    run_docs_build_worker,
    start_docs_build,
)
from data_engine.ui.gui.presenters.runtime_projection import (
    apply_daemon_snapshot,
    finish_daemon_startup,
)
from data_engine.ui.gui.presenters.workspace_binding import rebind_workspace_context
from data_engine.ui.gui.presenters.workspace_settings import (
    browse_workspace_collection_root_override,
    refresh_workspace_root_controls,
    reset_workspace_collection_root_override,
    save_workspace_collection_root_override,
)

__all__ = [
    "apply_daemon_snapshot",
    "browse_workspace_collection_root_override",
    "create_docs_browser",
    "docs_build_dir",
    "finish_daemon_startup",
    "finish_docs_build",
    "initialize_docs_view",
    "load_docs_page",
    "rebind_workspace_context",
    "refresh_workspace_root_controls",
    "reset_workspace_collection_root_override",
    "run_docs_build_worker",
    "save_workspace_collection_root_override",
    "start_docs_build",
]

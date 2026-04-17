"""Compatibility re-exports for the split workspace presenter helpers."""

from data_engine.ui.gui.presenters.docs import (
    create_docs_browser,
    initialize_docs_view,
    load_docs_page,
)
from data_engine.ui.gui.presenters.runtime_projection import (
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
    "browse_workspace_collection_root_override",
    "create_docs_browser",
    "finish_daemon_startup",
    "initialize_docs_view",
    "load_docs_page",
    "rebind_workspace_context",
    "refresh_workspace_root_controls",
    "reset_workspace_collection_root_override",
    "save_workspace_collection_root_override",
]

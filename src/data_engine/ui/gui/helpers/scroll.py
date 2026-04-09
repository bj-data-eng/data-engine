"""Scroll-surface helper functions for the desktop GUI."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def update_operation_scroll_cues(window: "DataEngineWindow", *args) -> None:
    del window, args


def update_sidebar_scroll_cues(window: "DataEngineWindow", *args) -> None:
    del window, args

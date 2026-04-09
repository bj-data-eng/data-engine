"""Scroll-surface helper functions for the desktop GUI."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def update_operation_scroll_cues(window: "DataEngineWindow", *args) -> None:
    del args
    scrollbar = window.operation_scroll.verticalScrollBar()
    maximum = scrollbar.maximum()
    value = scrollbar.value()
    has_overflow = maximum > 0
    window.operation_top_cue.setVisible(has_overflow and value > 0)
    window.operation_bottom_cue.setVisible(has_overflow and value < maximum)


def update_sidebar_scroll_cues(window: "DataEngineWindow", *args) -> None:
    del args
    scrollbar = window.sidebar_scroll.verticalScrollBar()
    maximum = scrollbar.maximum()
    value = scrollbar.value()
    has_overflow = maximum > 0
    window.sidebar_top_cue.setVisible(has_overflow and value > 0)
    window.sidebar_bottom_cue.setVisible(has_overflow and value < maximum)

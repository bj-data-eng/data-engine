"""Textual theme adapter over the shared Data Engine theme tokens."""

from __future__ import annotations

from data_engine.platform.theme import DEFAULT_THEME, THEMES, resolve_theme_name, system_theme_name


def stylesheet(theme_name: str = DEFAULT_THEME) -> str:
    """Return the TUI stylesheet for the requested theme."""
    palette = THEMES[resolve_theme_name(theme_name)]
    return f"""
    Screen {{
        layout: vertical;
        background: {palette.window_bg};
    }}

    #header {{
        height: 7;
        padding: 1 1 0 1;
        align: left middle;
    }}

    #header-copy {{
        width: 1fr;
        height: 1fr;
        layout: vertical;
    }}

    #header-actions {{
        width: auto;
        height: 1fr;
        align: right middle;
    }}

    #header-controls {{
        width: auto;
        height: auto;
        border: round {palette.panel_border};
        padding: 0 1;
        align: left middle;
        background: {palette.panel_bg};
    }}

    #screen-title {{
        width: 1fr;
        height: auto;
        text-style: bold;
        color: {palette.text};
    }}

    #screen-subtitle {{
        width: 1fr;
        height: auto;
        color: {palette.section_text};
    }}

    #status-line {{
        width: 1fr;
        height: auto;
        color: {palette.text};
        padding-top: 1;
    }}

    #control-status {{
        width: 1fr;
        height: auto;
        color: {palette.muted_text};
    }}

    #workspace-select {{
        width: 18;
        margin-left: 1;
    }}

    .pane-title {{
        height: auto;
        color: {palette.section_text};
        text-style: bold;
        padding-bottom: 1;
    }}

    .pane-toolbar {{
        height: 3;
        align: right middle;
    }}

    #body {{
        layout: grid;
        grid-size: 3 1;
        grid-columns: 40 1.7fr 1.3fr;
        grid-rows: 1fr;
        height: 1fr;
        padding: 1;
        grid-gutter: 1;
    }}

    #flow-list-pane {{
        height: 1fr;
        layout: vertical;
        border: round $surface;
        padding: 1;
        background: {palette.panel_bg};
    }}

    #detail-pane {{
        height: 1fr;
        layout: vertical;
        border: round $surface;
        padding: 1;
        background: {palette.panel_bg};
    }}

    #log-pane {{
        height: 1fr;
        layout: vertical;
        border: round $surface;
        padding: 1;
        background: {palette.panel_bg};
    }}

    #flow-list,
    #detail-view {{
        height: 1fr;
    }}

    #detail-view {{
        color: {palette.text};
    }}

    #flow-list {{
        padding: 0 1;
    }}

    #log-run-list {{
        height: 1fr;
        border: round {palette.panel_border};
        padding: 0 1;
        background: {palette.panel_bg};
    }}

    #flow-list .label {{
        height: auto;
        color: {palette.text};
    }}

    #flow-list > GroupHeaderListItem {{
        background: transparent;
        color: {palette.muted_text};
        text-style: bold;
        margin-top: 1;
        border-bottom: solid {palette.panel_border};
    }}

    #flow-list > GroupHeaderListItem.-disabled {{
        opacity: 1;
    }}

    #flow-list > FlowListItem {{
        padding: 0 1;
    }}

    #log-run-list .label {{
        height: auto;
        color: {palette.text};
    }}

    #log-run-list > .list-view--item-highlight {{
        background: {palette.selection_bg};
        color: {palette.selection_text};
    }}

    #flow-list > .list-view--item-highlight {{
        background: {palette.selection_bg};
        color: {palette.selection_text};
    }}

    Button {{
        margin-right: 1;
        height: 3;
        background: {palette.button_bg};
        color: {palette.text};
        border: round {palette.panel_border};
    }}

    Button:hover {{
        background: {palette.button_hover};
    }}

    Button:disabled {{
        background: {palette.button_disabled_bg};
        color: {palette.button_disabled_text};
        border: round {palette.button_disabled_border};
    }}

    #clear-flow-log {{
        margin-right: 0;
    }}
    """


TUI_CSS = stylesheet(DEFAULT_THEME)


__all__ = ["DEFAULT_THEME", "TUI_CSS", "resolve_theme_name", "stylesheet", "system_theme_name"]

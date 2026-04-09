from __future__ import annotations

from data_engine.platform.theme import DEFAULT_THEME, GITHUB_DARK, GITHUB_LIGHT, THEMES, resolve_theme_name, toggle_theme_name
from data_engine.ui.tui.theme import stylesheet as tui_stylesheet


def test_shared_theme_palettes_are_canonical():
    assert THEMES["dark"] == GITHUB_DARK
    assert THEMES["light"] == GITHUB_LIGHT
    assert GITHUB_DARK.text == "#c9d1d9"
    assert GITHUB_LIGHT.text == "#1f2328"


def test_shared_theme_name_helpers(monkeypatch):
    monkeypatch.setenv("DATA_ENGINE_THEME", "light")
    assert resolve_theme_name(DEFAULT_THEME) == "light"
    assert resolve_theme_name("dark") == "dark"
    assert toggle_theme_name("dark") == "light"


def test_tui_stylesheet_uses_shared_palette_tokens():
    dark_css = tui_stylesheet("dark")
    light_css = tui_stylesheet("light")

    assert GITHUB_DARK.window_bg in dark_css
    assert GITHUB_DARK.selection_bg in dark_css
    assert GITHUB_LIGHT.window_bg in light_css
    assert GITHUB_LIGHT.selection_bg in light_css

from __future__ import annotations

from data_engine.platform.theme import DEFAULT_THEME, GITHUB_DARK, GITHUB_LIGHT, THEMES, resolve_theme_name, toggle_theme_name


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

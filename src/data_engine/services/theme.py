"""Shared theme resolution services."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from data_engine.platform.theme import (
    DEFAULT_THEME,
    THEMES,
    ThemePalette,
    resolve_theme_name,
    system_theme_name,
    theme_button_text,
    toggle_theme_name,
)


class ThemeService:
    """Thin injectable wrapper around shared theme state decisions."""

    def __init__(
        self,
        *,
        themes: Mapping[str, ThemePalette] = THEMES,
        default_theme_name: str = DEFAULT_THEME,
        resolve_theme_name_func: Callable[[str], str] = resolve_theme_name,
        system_theme_name_func: Callable[[], str] = system_theme_name,
        toggle_theme_name_func: Callable[[str], str] = toggle_theme_name,
        theme_button_text_func: Callable[[str], str] = theme_button_text,
    ) -> None:
        self._themes = themes
        self.default_theme_name = default_theme_name
        self._resolve_theme_name = resolve_theme_name_func
        self._system_theme_name = system_theme_name_func
        self._toggle_theme_name = toggle_theme_name_func
        self._theme_button_text = theme_button_text_func

    def resolve_name(self, theme_name: str = DEFAULT_THEME) -> str:
        """Resolve one explicit or system-bound theme name."""
        return self._resolve_theme_name(theme_name)

    def system_name(self) -> str:
        """Return the host-system theme name."""
        return self._system_theme_name()

    def toggle_name(self, theme_name: str) -> str:
        """Return the opposite theme name."""
        return self._toggle_theme_name(theme_name)

    def button_text(self, theme_name: str) -> str:
        """Return the user-facing theme toggle text."""
        return self._theme_button_text(theme_name)

    def palette(self, theme_name: str = DEFAULT_THEME) -> ThemePalette:
        """Return the resolved semantic palette."""
        return self._themes[self.resolve_name(theme_name)]


__all__ = ["ThemeService"]

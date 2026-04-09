"""Shared theme tokens and host-theme detection across Data Engine surfaces."""

from __future__ import annotations

from dataclasses import dataclass
import os
import platform
import subprocess


@dataclass(frozen=True)
class ThemePalette:
    """One complete application palette."""

    name: str
    window_bg: str
    app_bg: str
    panel_bg: str
    panel_border: str
    text: str
    muted_text: str
    section_text: str
    accent_text: str
    warning_text: str
    error_text: str
    button_bg: str
    button_hover: str
    button_checked_bg: str
    button_checked_border: str
    button_disabled_bg: str
    button_disabled_border: str
    button_disabled_text: str
    input_bg: str
    input_border: str
    hover_bg: str
    hover_border: str
    selection_bg: str
    selection_text: str
    selection_border: str
    tab_bg: str
    tab_hover_bg: str
    tab_selected_bg: str
    progress_bg: str
    progress_chunk: str
    summary_bg: str
    summary_border: str
    request_control_bg: str
    request_control_border: str
    request_control_hover: str
    engine_start_bg: str
    engine_start_border: str
    engine_start_hover: str
    engine_stop_bg: str
    engine_stop_border: str
    engine_stop_hover: str


GITHUB_DARK = ThemePalette(
    name="dark",
    window_bg="#0d1117",
    app_bg="#0d1117",
    panel_bg="#161b22",
    panel_border="#30363d",
    text="#c9d1d9",
    muted_text="#8b949e",
    section_text="#7d8590",
    accent_text="#2ea043",
    warning_text="#d29922",
    error_text="#f85149",
    button_bg="#21262d",
    button_hover="#30363d",
    button_checked_bg="#1f6feb",
    button_checked_border="#388bfd",
    button_disabled_bg="#161b22",
    button_disabled_border="#30363d",
    button_disabled_text="#6e7681",
    input_bg="#0d1117",
    input_border="#30363d",
    hover_bg="#1b2230",
    hover_border="#3b4556",
    selection_bg="#1f6feb",
    selection_text="#f0f6fc",
    selection_border="#388bfd",
    tab_bg="#161b22",
    tab_hover_bg="#1b2230",
    tab_selected_bg="#21262d",
    progress_bg="#0d1117",
    progress_chunk="#2ea043",
    summary_bg="#21262d",
    summary_border="#30363d",
    request_control_bg="#F04A00",
    request_control_border="#c23c00",
    request_control_hover="#d84300",
    engine_start_bg="#1f883d",
    engine_start_border="#1a7f37",
    engine_start_hover="#1a7f37",
    engine_stop_bg="#cf222e",
    engine_stop_border="#a40e26",
    engine_stop_hover="#a40e26",
)

GITHUB_LIGHT = ThemePalette(
    name="light",
    window_bg="#ffffff",
    app_bg="#f6f8fa",
    panel_bg="#ffffff",
    panel_border="#d0d7de",
    text="#1f2328",
    muted_text="#656d76",
    section_text="#57606a",
    accent_text="#1a7f37",
    warning_text="#9a6700",
    error_text="#cf222e",
    button_bg="#f6f8fa",
    button_hover="#eef2f6",
    button_checked_bg="#ddf4ff",
    button_checked_border="#54aeff",
    button_disabled_bg="#f6f8fa",
    button_disabled_border="#d8dee4",
    button_disabled_text="#8c959f",
    input_bg="#ffffff",
    input_border="#d0d7de",
    hover_bg="#f6f8fa",
    hover_border="#c7d2dd",
    selection_bg="#0969da",
    selection_text="#ffffff",
    selection_border="#54aeff",
    tab_bg="#f6f8fa",
    tab_hover_bg="#eef2f6",
    tab_selected_bg="#ffffff",
    progress_bg="#eef2f6",
    progress_chunk="#1a7f37",
    summary_bg="#f6f8fa",
    summary_border="#d0d7de",
    request_control_bg="#F04A00",
    request_control_border="#c23c00",
    request_control_hover="#d84300",
    engine_start_bg="#1f883d",
    engine_start_border="#1a7f37",
    engine_start_hover="#1a7f37",
    engine_stop_bg="#cf222e",
    engine_stop_border="#a40e26",
    engine_stop_hover="#a40e26",
)

THEMES = {
    "dark": GITHUB_DARK,
    "light": GITHUB_LIGHT,
}

DEFAULT_THEME = "system"


def toggle_theme_name(theme_name: str) -> str:
    """Return the opposite theme name."""
    return "light" if theme_name == "dark" else "dark"


def theme_button_text(theme_name: str) -> str:
    """Return the user-facing label for the theme toggle button."""
    return "Switch to Light" if theme_name == "dark" else "Switch to Dark"


def _qt_theme_name() -> str | None:
    try:
        from PySide6.QtCore import Qt  # type: ignore
        from PySide6.QtGui import QGuiApplication  # type: ignore
    except Exception:
        return None
    app = QGuiApplication.instance()
    if app is None:
        return None
    try:
        scheme = app.styleHints().colorScheme()
    except AttributeError:
        return None
    return "dark" if scheme == Qt.ColorScheme.Dark else "light"


def _macos_theme_name() -> str | None:
    try:
        result = subprocess.run(
            ["defaults", "read", "-g", "AppleInterfaceStyle"],
            check=False,
            capture_output=True,
            text=True,
            timeout=0.5,
        )
    except Exception:
        return None
    return "dark" if "dark" in (result.stdout or "").strip().lower() else "light"


def _windows_theme_name() -> str | None:
    try:
        import winreg  # type: ignore
    except Exception:
        return None
    try:
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize",
        ) as key:
            value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
    except Exception:
        return None
    return "light" if int(value) else "dark"


def _linux_theme_name() -> str | None:
    gtk_theme = os.environ.get("GTK_THEME", "").lower()
    if ":dark" in gtk_theme or gtk_theme.endswith("-dark"):
        return "dark"
    if gtk_theme:
        return "light"
    colorfgbg = os.environ.get("COLORFGBG", "")
    if colorfgbg:
        try:
            bg = int(colorfgbg.split(";")[-1])
        except ValueError:
            return None
        return "dark" if bg <= 6 else "light"
    return None


def system_theme_name() -> str:
    """Return the host light/dark theme using shared cross-surface detection."""
    override = os.environ.get("DATA_ENGINE_THEME", "").strip().lower()
    if override in THEMES:
        return override
    qt_theme = _qt_theme_name()
    if qt_theme is not None:
        return qt_theme
    system = platform.system()
    if system == "Darwin":
        return _macos_theme_name() or "dark"
    if system == "Windows":
        return _windows_theme_name() or "dark"
    return _linux_theme_name() or "dark"


def resolve_theme_name(theme_name: str = DEFAULT_THEME) -> str:
    """Resolve a requested theme name, honoring system-following default behavior."""
    if theme_name == DEFAULT_THEME:
        return system_theme_name()
    return theme_name if theme_name in THEMES else system_theme_name()


__all__ = [
    "DEFAULT_THEME",
    "GITHUB_DARK",
    "GITHUB_LIGHT",
    "THEMES",
    "ThemePalette",
    "resolve_theme_name",
    "system_theme_name",
    "theme_button_text",
    "toggle_theme_name",
]

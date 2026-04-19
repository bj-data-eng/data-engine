"""Launcher entrypoints for the Rust-backed egui surface package."""

from __future__ import annotations

from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.ui.egui.home_state import EguiHomeStateProvider
from data_engine.ui.egui.native import launch_native


def launch(theme_name: str | None = None) -> None:
    """Launch the Rust-backed egui surface."""
    del theme_name
    provider = EguiHomeStateProvider(title=f"{APP_DISPLAY_NAME} egui")
    try:
        launch_native(title=f"{APP_DISPLAY_NAME} egui", home_provider=provider)
    finally:
        provider.close()


def main() -> None:
    """Console entrypoint for the experimental egui surface."""
    launch()


if __name__ == "__main__":
    main()


__all__ = ["launch", "main"]

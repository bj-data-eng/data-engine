"""UI icon registry and loading helpers."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources


@dataclass(frozen=True)
class SvgIconAsset:
    """Describe one SVG icon source."""

    file_name: str | None = None
    svg_text: str | None = None

    def read_text(self) -> str:
        """Return raw SVG text for this asset."""
        if self.svg_text is not None:
            return self.svg_text
        if self.file_name is None:
            raise ValueError("SVG icon asset has no source configured.")
        return resources.files("data_engine.ui.gui").joinpath("icons", self.file_name).read_text(encoding="utf-8")


ICON_ASSETS: dict[str, SvgIconAsset] = {
    "dark_light": SvgIconAsset(file_name="dark_light.svg"),
    "debug": SvgIconAsset(file_name="debug.svg"),
    "documentation": SvgIconAsset(file_name="documentation.svg"),
    "failed": SvgIconAsset(file_name="failed.svg"),
    "group": SvgIconAsset(file_name="group.svg"),
    "home": SvgIconAsset(file_name="home.svg"),
    "manual": SvgIconAsset(file_name="manual.svg"),
    "poll": SvgIconAsset(file_name="poll.svg"),
    "schedule": SvgIconAsset(file_name="schedule.svg"),
    "settings": SvgIconAsset(file_name="settings.svg"),
    "started": SvgIconAsset(file_name="started.svg"),
    "success": SvgIconAsset(file_name="success.svg"),
    "view-log": SvgIconAsset(file_name="view-log.svg"),
}


def load_svg_icon_text(icon_name: str) -> str:
    """Return SVG text for one registered icon."""
    try:
        asset = ICON_ASSETS[icon_name]
    except KeyError as exc:
        raise KeyError(f"Unknown UI icon: {icon_name}") from exc
    return asset.read_text()


__all__ = ["ICON_ASSETS", "SvgIconAsset", "load_svg_icon_text"]

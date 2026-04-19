"""Python wrapper for the Rust-backed egui extension module."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType


def _import_first(*names: str) -> ModuleType:
    """Import the first available native egui module candidate."""
    last_error: ModuleNotFoundError | None = None
    for name in names:
        try:
            return import_module(name)
        except ModuleNotFoundError as error:
            last_error = error
    if last_error is not None:
        raise last_error
    raise ModuleNotFoundError("No egui native module candidates were provided.")


def _module() -> ModuleType:
    """Return the installed Rust-backed egui module."""
    return _import_first(
        "data_engine.ui.egui._data_engine_egui",
        "_data_engine_egui",
    )


def hello() -> str:
    """Return a small diagnostic string from the native egui module."""
    return str(_module().hello())


def launch_native(*, title: str | None = None, home_provider=None) -> None:
    """Launch the Rust-backed egui surface."""
    _module().launch(title=title, home_provider=home_provider)


def runtime_info():
    """Return native runtime metadata from the Rust egui module."""
    return _module().runtime_info()


__all__ = ["hello", "launch_native", "runtime_info"]

"""CLI surface package."""

from typing import Any


_APP_EXPORTS = {
    "CliDependencies",
    "CliDependencyFactories",
    "build_default_cli_dependencies",
    "build_parser",
    "default_cli_dependency_factories",
    "main",
}


def __getattr__(name: str) -> Any:
    """Load CLI app exports lazily so ``python -m data_engine.ui.cli.app`` is clean."""
    if name in _APP_EXPORTS:
        from data_engine.ui.cli import app

        return getattr(app, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "CliDependencies",
    "CliDependencyFactories",
    "build_default_cli_dependencies",
    "build_parser",
    "default_cli_dependency_factories",
    "main",
]

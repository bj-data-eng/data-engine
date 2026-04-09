"""Qt GUI surface for Data Engine."""

from data_engine.domain import format_log_line
from data_engine.views import QtFlowCard, flow_category

__all__ = [
    "DataEngineWindow",
    "QtFlowCard",
    "flow_category",
    "format_log_line",
    "launch",
    "main",
]


def __getattr__(name: str):
    if name == "DataEngineWindow":
        from data_engine.ui.gui.app import DataEngineWindow

        return DataEngineWindow
    if name in {"launch", "main"}:
        from data_engine.ui.gui.launcher import launch, main

        return {"launch": launch, "main": main}[name]
    raise AttributeError(name)

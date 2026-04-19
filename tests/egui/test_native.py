from __future__ import annotations

from types import SimpleNamespace

from data_engine.ui.egui import native


def test_native_module_prefers_nested_package(monkeypatch):
    nested = SimpleNamespace()

    def _import_module(name: str):
        if name == "data_engine.ui.egui._data_engine_egui":
            return nested
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(native, "import_module", _import_module)

    assert native._module() is nested


def test_native_module_falls_back_to_top_level_package(monkeypatch):
    top_level = SimpleNamespace(hello=lambda: "hello", runtime_info=lambda: SimpleNamespace(version="0.1.0"))

    def _import_module(name: str):
        if name == "data_engine.ui.egui._data_engine_egui":
            raise ModuleNotFoundError(name)
        if name == "_data_engine_egui":
            return top_level
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(native, "import_module", _import_module)

    assert native.hello() == "hello"
    assert native.runtime_info().version == "0.1.0"


def test_launch_native_passes_home_provider(monkeypatch):
    payload: dict[str, object] = {}
    top_level = SimpleNamespace(
        launch=lambda **kwargs: payload.update(kwargs),
    )

    monkeypatch.setattr(native, "_module", lambda: top_level)

    provider = object()
    native.launch_native(title="Example", home_provider=provider)

    assert payload == {"title": "Example", "home_provider": provider}

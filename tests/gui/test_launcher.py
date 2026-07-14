from __future__ import annotations

from types import SimpleNamespace

from data_engine.ui.gui import launcher


class _FakeApplication:
    def __init__(self) -> None:
        self.application_names: list[str] = []
        self.styles: list[str] = []
        self.stylesheets: list[str] = []
        self.exec_calls = 0

    def setApplicationName(self, name: str) -> None:
        self.application_names.append(name)

    def setStyle(self, style: str) -> None:
        self.styles.append(style)

    def setStyleSheet(self, value: str) -> None:
        self.stylesheets.append(value)

    def exec(self) -> int:
        self.exec_calls += 1
        return 0


class _ApplicationFactory:
    def __init__(self, existing: _FakeApplication | None) -> None:
        self.current = existing
        self.created_arguments: list[list[str]] = []

    def instance(self) -> _FakeApplication | None:
        return self.current

    def __call__(self, arguments: list[str]) -> _FakeApplication:
        self.created_arguments.append(arguments)
        self.current = _FakeApplication()
        return self.current


class _FakeWindow:
    def __init__(self, *, theme_name: str, services: object) -> None:
        self.theme_name = theme_name
        self.services = services
        self.show_calls = 0

    def show(self) -> None:
        self.show_calls += 1


def _stub_launcher(monkeypatch, *, existing: _FakeApplication | None):
    application_factory = _ApplicationFactory(existing)
    services = SimpleNamespace(theme_service=SimpleNamespace(resolve_name=lambda name: f"resolved-{name}"))
    windows: list[_FakeWindow] = []

    def create_window(*, theme_name: str, services: object) -> _FakeWindow:
        window = _FakeWindow(theme_name=theme_name, services=services)
        windows.append(window)
        return window

    monkeypatch.setattr(launcher, "QApplication", application_factory)
    monkeypatch.setattr(launcher, "build_gui_services", lambda: services)
    monkeypatch.setattr(launcher, "DataEngineWindow", create_window)
    monkeypatch.setattr(launcher, "stylesheet", lambda theme_name: f"css-{theme_name}")
    monkeypatch.setattr(launcher, "_configure_qt_webengine_environment", lambda: None)
    return application_factory, services, windows


def test_launch_returns_window_without_running_existing_application(monkeypatch) -> None:
    existing = _FakeApplication()
    application_factory, services, windows = _stub_launcher(monkeypatch, existing=existing)

    window = launcher.launch(theme_name="dark")

    assert window is windows[0]
    assert window.services is services
    assert window.theme_name == "resolved-dark"
    assert window.show_calls == 1
    assert application_factory.created_arguments == []
    assert existing.exec_calls == 0
    assert existing.stylesheets == ["css-resolved-dark"]


def test_launch_runs_application_event_loop_when_it_owns_application(monkeypatch) -> None:
    application_factory, services, windows = _stub_launcher(monkeypatch, existing=None)

    window = launcher.launch(theme_name="light")

    assert window is windows[0]
    assert window.services is services
    assert window.show_calls == 1
    assert application_factory.created_arguments == [[]]
    assert application_factory.current is not None
    assert application_factory.current.exec_calls == 1

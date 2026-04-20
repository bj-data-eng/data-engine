from __future__ import annotations

import os
from argparse import Namespace
from pathlib import Path
import sys

import pytest

from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR, DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR
from data_engine.ui.cli.app import (
    _apply_environment,
    _infer_project_root_from_cwd,
    _test_slice_args,
)
from data_engine.ui.cli.commands_run import checkout_tests_dir, raise_open_file_limit
from data_engine.ui.cli.commands_workspace import (
    collection_vscode_settings as _collection_vscode_settings,
    workspace_vscode_settings as _workspace_vscode_settings,
)
from data_engine.ui.cli.parser import (
    _HelpFormatter,
    build_parser,
)


def test_build_parser_exposes_new_public_commands():
    parser = build_parser()

    assert type(parser._get_formatter()) is _HelpFormatter
    assert parser.parse_args(["start", "gui"]).start_command == "gui"
    assert parser.parse_args(["run", "gui"]).run_command == "gui"
    assert parser.parse_args(["run", "egui"]).run_command == "egui"
    assert parser.parse_args(["run", "tui"]).run_command == "tui"
    assert parser.parse_args(["create", "workspace", "/tmp/example"]).create_command == "workspace"
    assert parser.parse_args(["run", "tests"]).slice == "unit"
    assert parser.parse_args(["run", "tests", "all"]).slice == "all"
    assert parser.parse_args(["doctor", "daemons"]).doctor_command == "daemons"


def test_infer_project_root_from_cwd_detects_checkout_layout(tmp_path):
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "pyproject.toml").write_text("[build-system]\n", encoding="utf-8")
    (project_root / "src" / "data_engine").mkdir(parents=True)

    assert _infer_project_root_from_cwd(project_root) == project_root
    assert _infer_project_root_from_cwd(tmp_path / "elsewhere") is None


def test_apply_environment_sets_explicit_paths(monkeypatch, tmp_path):
    monkeypatch.delenv(DATA_ENGINE_APP_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)

    args = Namespace(app_root=tmp_path / "app-root", workspace=tmp_path / "workspace-root")
    _apply_environment(args)

    assert Path(os.environ[DATA_ENGINE_APP_ROOT_ENV_VAR]) == (tmp_path / "app-root").resolve()
    assert Path(os.environ[DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR]) == (tmp_path / "workspace-root").resolve()


def test_apply_environment_infers_app_root_from_cwd(monkeypatch, tmp_path):
    monkeypatch.delenv(DATA_ENGINE_APP_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)

    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "pyproject.toml").write_text("[build-system]\n", encoding="utf-8")
    (project_root / "src" / "data_engine").mkdir(parents=True)
    monkeypatch.chdir(project_root)

    _apply_environment(Namespace(app_root=None, workspace=None))

    assert Path(os.environ[DATA_ENGINE_APP_ROOT_ENV_VAR]) == project_root.resolve()
    assert DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR not in os.environ


def test_workspace_vscode_settings_point_back_to_shared_app_root(tmp_path, monkeypatch):
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    app_root = tmp_path / "data_engine"
    (app_root / "src").mkdir(parents=True)
    (app_root / "tests").mkdir(parents=True)
    settings = _workspace_vscode_settings(tmp_path / "workspaces" / "claims", app_root=app_root)

    assert settings["python.analysis.extraPaths"] == [str(app_root / "src")]
    assert settings["python.testing.pytestArgs"] == [str(app_root / "tests")]
    assert settings["terminal.integrated.env.osx"]["DATA_ENGINE_WORKSPACE_ID"] == "claims"
    assert settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_ID"] == "claims"
    assert settings["terminal.integrated.env.windows"] == settings["terminal.integrated.env.osx"]


def test_collection_vscode_settings_point_back_to_shared_app_root(tmp_path, monkeypatch):
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    app_root = tmp_path / "data_engine"
    collection_root = tmp_path / "workspaces"
    (app_root / "src").mkdir(parents=True)
    (app_root / "tests").mkdir(parents=True)
    settings = _collection_vscode_settings(collection_root, app_root=app_root)

    assert settings["python.analysis.extraPaths"] == [str(app_root / "src")]
    assert settings["python.testing.pytestArgs"] == [str(app_root / "tests")]
    assert settings["terminal.integrated.env.osx"]["DATA_ENGINE_WORKSPACE_COLLECTION_ROOT"] == str(collection_root)
    assert settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_COLLECTION_ROOT"] == str(collection_root)
    assert settings["terminal.integrated.env.windows"] == settings["terminal.integrated.env.osx"]


def test_test_slice_args_cover_named_human_slices(tmp_path):
    app_root = tmp_path / "data_engine"
    tests_dir = app_root / "tests"
    tests_dir.mkdir(parents=True)

    assert _test_slice_args("all", app_root=app_root) == (str(tests_dir),)
    assert _test_slice_args("qt", app_root=app_root) == (str(tests_dir / "gui" / "qt"),)
    assert _test_slice_args("tui", app_root=app_root) == (str(tests_dir / "tui"),)
    assert _test_slice_args("integration", app_root=app_root) == (str(tests_dir / "integration"),)
    assert _test_slice_args("live", app_root=app_root) == (str(tests_dir / "daemon" / "test_live_runtime_suite.py"),)
    unit_args = _test_slice_args("unit", app_root=app_root)
    assert str(tests_dir) in unit_args
    assert any("--ignore=" in arg for arg in unit_args)


def test_checkout_tests_dir_requires_checkout_layout(tmp_path):
    app_root = tmp_path / "installed_app"

    with pytest.raises(Exception, match="checkout-style app root"):
        checkout_tests_dir(app_root)


def test_raise_open_file_limit_raises_soft_limit_when_resource_allows(monkeypatch):
    calls: list[tuple[int, tuple[int, int]]] = []

    class _FakeResource:
        RLIMIT_NOFILE = 1

        @staticmethod
        def getrlimit(_kind):
            return (256, 1024)

        @staticmethod
        def setrlimit(kind, value):
            calls.append((kind, value))

    monkeypatch.setitem(sys.modules, "resource", _FakeResource)

    raise_open_file_limit()

    assert calls == [(_FakeResource.RLIMIT_NOFILE, (1024, 1024))]


def test_raise_open_file_limit_uses_minimum_when_hard_limit_is_unlimited(monkeypatch):
    calls: list[tuple[int, tuple[int, int]]] = []

    class _FakeResource:
        RLIMIT_NOFILE = 1

        @staticmethod
        def getrlimit(_kind):
            return (256, -1)

        @staticmethod
        def setrlimit(kind, value):
            calls.append((kind, value))

    monkeypatch.setitem(sys.modules, "resource", _FakeResource)

    raise_open_file_limit()

    assert calls == [(_FakeResource.RLIMIT_NOFILE, (4096, -1))]

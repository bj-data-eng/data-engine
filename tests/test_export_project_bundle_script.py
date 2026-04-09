from __future__ import annotations

import importlib.util
import io
from pathlib import Path
from zipfile import ZipFile


def _load_bundle_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "export_project_bundle_script.py"
    spec = importlib.util.spec_from_file_location("export_project_bundle_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_archive_includes_project_source_and_excludes_artifacts_and_workspace_state(tmp_path):
    module = _load_bundle_module()

    root = tmp_path / "data_engine"
    workspaces = tmp_path / "workspaces"
    (tmp_path / "repo.code-workspace").write_text('{"folders":[{"path":"data_engine"}]}\n', encoding="utf-8")
    (root / "src" / "data_engine" / "docs" / "sphinx_source").mkdir(parents=True)
    (root / "src" / "data_engine" / "docs" / "sphinx_source" / "index.rst").write_text("Docs", encoding="utf-8")
    (root / "src" / "data_engine" / "module.py").write_text("x = 1\n", encoding="utf-8")
    (root / "INSTALL").mkdir(parents=True)
    (root / "INSTALL" / "INSTALL MAC.command").write_text("install\n", encoding="utf-8")
    (root / "INSTALL" / "INSTALL WINDOWS.bat").write_text("install\r\n", encoding="utf-8")
    (root / "INSTALL" / "INSTALL WINDOWS_VM.bat").write_text("install\r\n", encoding="utf-8")
    (workspaces / "example_workspace" / "flow_modules").mkdir(parents=True)
    (workspaces / "example_workspace" / "flow_modules" / "demo.ipynb").write_text("{}", encoding="utf-8")
    (workspaces / "claims2" / "flow_modules").mkdir(parents=True)
    (workspaces / "claims2" / "flow_modules" / "other.ipynb").write_text("{}", encoding="utf-8")
    (workspaces / "example_workspace" / ".workspace_state" / "leased").mkdir(parents=True)
    (workspaces / "example_workspace" / ".workspace_state" / "leased" / "example_workspace").write_text("owned\n", encoding="utf-8")
    (root / "artifacts" / "documentation" / "_build" / "html").mkdir(parents=True)
    (root / "artifacts" / "documentation" / "_build" / "html" / "index.html").write_text("<h1>built</h1>", encoding="utf-8")
    (root / "artifacts" / "workspace_cache" / "example_workspace" / "compiled_flow_modules").mkdir(parents=True)
    (root / "artifacts" / "workspace_cache" / "example_workspace" / "compiled_flow_modules" / "demo.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "artifacts" / "runtime_state" / "example_workspace").mkdir(parents=True)
    (root / "artifacts" / "runtime_state" / "example_workspace" / "runtime_ledger.sqlite").write_text("db\n", encoding="utf-8")

    archive_bytes = module.build_archive(root=root, output_file=root / "project_bundle.py")

    with ZipFile(io.BytesIO(archive_bytes)) as archive:
        members = set(archive.namelist())

    assert "src/data_engine/docs/sphinx_source/index.rst" in members
    assert "src/data_engine/module.py" in members
    assert "INSTALL/INSTALL MAC.command" in members
    assert "INSTALL/INSTALL WINDOWS.bat" in members
    assert "INSTALL/INSTALL WINDOWS_VM.bat" in members
    assert "repo.code-workspace" in members
    assert "workspaces/example_workspace/flow_modules/demo.ipynb" not in members
    assert "workspaces/claims2/flow_modules/other.ipynb" not in members
    assert "artifacts/workspace_cache/example_workspace/compiled_flow_modules/demo.py" not in members
    assert "artifacts/runtime_state/example_workspace/runtime_ledger.sqlite" not in members
    assert "artifacts/documentation/_build/html/index.html" not in members
    assert "workspaces/example_workspace/.workspace_state/leased/example_workspace" not in members


def test_build_archive_code_bundle_excludes_tests_but_keeps_workspaces(tmp_path):
    module = _load_bundle_module()

    root = tmp_path / "data_engine"
    workspaces = tmp_path / "workspaces"
    (tmp_path / "repo.code-workspace").write_text('{"folders":[{"path":"data_engine"}]}\n', encoding="utf-8")
    (root / "src" / "data_engine").mkdir(parents=True)
    (root / "src" / "data_engine" / "module.py").write_text("x = 1\n", encoding="utf-8")
    (root / "INSTALL").mkdir(parents=True)
    (root / "INSTALL" / "INSTALL MAC.command").write_text("install\n", encoding="utf-8")
    (root / "INSTALL" / "INSTALL WINDOWS.bat").write_text("install\r\n", encoding="utf-8")
    (root / "INSTALL" / "INSTALL WINDOWS_VM.bat").write_text("install\r\n", encoding="utf-8")
    (root / "tests").mkdir(parents=True)
    (root / "tests" / "test_demo.py").write_text("def test_demo(): pass\n", encoding="utf-8")
    (workspaces / "example_workspace" / "flow_modules").mkdir(parents=True)
    (workspaces / "example_workspace" / "flow_modules" / "demo.py").write_text("FLOW = 1\n", encoding="utf-8")
    (workspaces / "claims2" / "flow_modules").mkdir(parents=True)
    (workspaces / "claims2" / "flow_modules" / "other.py").write_text("FLOW = 2\n", encoding="utf-8")

    archive_bytes = module.build_archive(root=root, output_file=root / "project_code_bundle.py", bundle_kind="code")

    with ZipFile(io.BytesIO(archive_bytes)) as archive:
        members = set(archive.namelist())

    assert "src/data_engine/module.py" in members
    assert "INSTALL/INSTALL MAC.command" in members
    assert "INSTALL/INSTALL WINDOWS.bat" in members
    assert "INSTALL/INSTALL WINDOWS_VM.bat" in members
    assert "repo.code-workspace" in members
    assert "workspaces/example_workspace/flow_modules/demo.py" not in members
    assert "workspaces/claims2/flow_modules/other.py" not in members
    assert "tests/test_demo.py" not in members


def test_build_archive_can_target_a_different_single_workspace(tmp_path):
    module = _load_bundle_module()

    root = tmp_path / "data_engine"
    workspaces = tmp_path / "workspaces"
    (root / "src" / "data_engine").mkdir(parents=True)
    (root / "src" / "data_engine" / "module.py").write_text("x = 1\n", encoding="utf-8")
    (workspaces / "example_workspace" / "flow_modules").mkdir(parents=True)
    (workspaces / "example_workspace" / "flow_modules" / "demo.py").write_text("FLOW = 1\n", encoding="utf-8")
    (workspaces / "claims2" / "flow_modules").mkdir(parents=True)
    (workspaces / "claims2" / "flow_modules" / "other.py").write_text("FLOW = 2\n", encoding="utf-8")

    archive_bytes = module.build_archive(
        root=root,
        output_file=root / "project_bundle.py",
        workspace_id="claims2",
    )

    with ZipFile(io.BytesIO(archive_bytes)) as archive:
        members = set(archive.namelist())

    assert "workspaces/example_workspace/flow_modules/demo.py" not in members
    assert "workspaces/claims2/flow_modules/other.py" not in members


def test_build_archive_tests_bundle_only_includes_tests(tmp_path):
    module = _load_bundle_module()

    root = tmp_path / "data_engine"
    workspaces = tmp_path / "workspaces"
    (tmp_path / "repo.code-workspace").write_text('{"folders":[{"path":"data_engine"}]}\n', encoding="utf-8")
    (root / "src" / "data_engine").mkdir(parents=True)
    (root / "src" / "data_engine" / "module.py").write_text("x = 1\n", encoding="utf-8")
    (root / "tests").mkdir(parents=True)
    (root / "tests" / "test_demo.py").write_text("def test_demo(): pass\n", encoding="utf-8")
    (workspaces / "example_workspace" / "flow_modules").mkdir(parents=True)
    (workspaces / "example_workspace" / "flow_modules" / "demo.py").write_text("FLOW = 1\n", encoding="utf-8")

    archive_bytes = module.build_archive(root=root, output_file=root / "project_tests_bundle.py", bundle_kind="tests")

    with ZipFile(io.BytesIO(archive_bytes)) as archive:
        members = set(archive.namelist())

    assert "tests/test_demo.py" in members
    assert "repo.code-workspace" not in members
    assert "src/data_engine/module.py" not in members
    assert "workspaces/example_workspace/flow_modules/demo.py" not in members

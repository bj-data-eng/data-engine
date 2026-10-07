from __future__ import annotations

import os

from data_engine.flow_modules.flow_module_compiler import prepare_flow_modules
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def test_prepare_flow_modules_removes_only_auto_compiled_orphans(tmp_path):
    flow_modules_dir = tmp_path / "workspace" / "flow_modules"
    compiled_flow_modules_dir = resolve_workspace_paths(workspace_root=tmp_path / "workspace").compiled_flow_modules_dir
    flow_modules_dir.mkdir(parents=True)
    compiled_flow_modules_dir.mkdir(parents=True)

    (flow_modules_dir / "example.py").write_text(
        "from data_engine import Flow\ndef build():\n"
        "    return Flow(group='Tests').step(lambda context: context.current)\n",
        encoding="utf-8",
    )
    (compiled_flow_modules_dir / "orphan.py").write_text(
        '"""Auto-compiled flow module. Source notebook is authoritative."""\n\ndef build():\n    return None\n',
        encoding="utf-8",
    )
    (compiled_flow_modules_dir / "_helpers.py").write_text("HELPER = True\n", encoding="utf-8")
    (compiled_flow_modules_dir / "manual_extra.py").write_text("VALUE = 1\n", encoding="utf-8")

    prepare_flow_modules(data_root=tmp_path / "workspace")

    assert not (compiled_flow_modules_dir / "orphan.py").exists()
    assert (compiled_flow_modules_dir / "_helpers.py").exists()
    assert (compiled_flow_modules_dir / "manual_extra.py").exists()
    assert (compiled_flow_modules_dir / "example.py").exists()


def test_prepare_flow_modules_mirrors_authored_python_modules(tmp_path):
    flow_modules_dir = tmp_path / "workspace" / "flow_modules"
    compiled_flow_modules_dir = resolve_workspace_paths(workspace_root=tmp_path / "workspace").compiled_flow_modules_dir
    flow_modules_dir.mkdir(parents=True)
    compiled_flow_modules_dir.mkdir(parents=True)

    source_path = flow_modules_dir / "python_flow.py"
    source_path.write_text(
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="python_flow", label="Python Flow", group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    compiled = prepare_flow_modules(data_root=tmp_path / "workspace")
    mirrored_path = compiled_flow_modules_dir / "python_flow.py"

    assert mirrored_path.exists()
    rendered = mirrored_path.read_text(encoding="utf-8")
    assert "Mirrored flow module" in rendered
    assert 'label="Python Flow"' in rendered
    assert [item.name for item in compiled] == ["python_flow"]


def test_prepare_flow_modules_recompiles_when_source_changes_without_newer_mtime(tmp_path):
    flow_modules_dir = tmp_path / "workspace" / "flow_modules"
    compiled_flow_modules_dir = resolve_workspace_paths(workspace_root=tmp_path / "workspace").compiled_flow_modules_dir
    flow_modules_dir.mkdir(parents=True)
    compiled_flow_modules_dir.mkdir(parents=True)

    source_path = flow_modules_dir / "python_flow.py"
    source_path.write_text(
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="python_flow", label="Original", group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    first = prepare_flow_modules(data_root=tmp_path / "workspace")
    assert [item.name for item in first] == ["python_flow"]

    compiled_path = compiled_flow_modules_dir / "python_flow.py"
    compiled_mtime = compiled_path.stat().st_mtime
    source_path.write_text(
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="python_flow", label="Updated", group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )
    os.utime(source_path, (compiled_mtime, compiled_mtime))

    second = prepare_flow_modules(data_root=tmp_path / "workspace")

    assert [item.name for item in second] == ["python_flow"]
    assert 'label="Updated"' in compiled_path.read_text(encoding="utf-8")


def test_prepare_flow_modules_mirrors_flow_helpers_package(tmp_path):
    flow_modules_dir = tmp_path / "workspace" / "flow_modules"
    compiled_flow_modules_dir = resolve_workspace_paths(workspace_root=tmp_path / "workspace").compiled_flow_modules_dir
    helper_modules_dir = flow_modules_dir / "flow_helpers"
    helper_modules_dir.mkdir(parents=True)
    compiled_flow_modules_dir.mkdir(parents=True)

    (helper_modules_dir / "labels.py").write_text("FLOW_LABEL = 'Helper Demo'\n", encoding="utf-8")

    prepare_flow_modules(data_root=tmp_path / "workspace")

    assert (compiled_flow_modules_dir / "flow_helpers" / "__init__.py").is_file()
    assert (compiled_flow_modules_dir / "flow_helpers" / "labels.py").read_text(encoding="utf-8") == "FLOW_LABEL = 'Helper Demo'\n"


def test_prepare_flow_modules_updates_flow_helpers_in_place_without_renaming_package_dir(tmp_path, monkeypatch):
    flow_modules_dir = tmp_path / "workspace" / "flow_modules"
    compiled_flow_modules_dir = resolve_workspace_paths(workspace_root=tmp_path / "workspace").compiled_flow_modules_dir
    helper_modules_dir = flow_modules_dir / "flow_helpers"
    helper_modules_dir.mkdir(parents=True)
    compiled_helper_modules_dir = compiled_flow_modules_dir / "flow_helpers"
    compiled_helper_modules_dir.mkdir(parents=True)

    (helper_modules_dir / "labels.py").write_text("FLOW_LABEL = 'Updated Demo'\n", encoding="utf-8")
    (compiled_helper_modules_dir / "labels.py").write_text("FLOW_LABEL = 'Original Demo'\n", encoding="utf-8")
    (compiled_helper_modules_dir / "orphan.py").write_text("ORPHAN = True\n", encoding="utf-8")
    (compiled_helper_modules_dir / "__init__.py").write_text('"""Existing helpers."""\n', encoding="utf-8")

    original_rename = type(compiled_helper_modules_dir).rename

    def _guarded_rename(path, target):
        if path == compiled_helper_modules_dir:
            raise AssertionError("flow_helpers package directory should not be renamed during mirroring")
        return original_rename(path, target)

    monkeypatch.setattr(type(compiled_helper_modules_dir), "rename", _guarded_rename)

    prepare_flow_modules(data_root=tmp_path / "workspace")

    assert compiled_helper_modules_dir.exists() is True
    assert (compiled_helper_modules_dir / "labels.py").read_text(encoding="utf-8") == "FLOW_LABEL = 'Updated Demo'\n"
    assert (compiled_helper_modules_dir / "orphan.py").exists() is False


def test_prepare_flow_modules_removes_orphaned_helper_directories_even_when_not_empty(tmp_path):
    flow_modules_dir = tmp_path / "workspace" / "flow_modules"
    compiled_flow_modules_dir = resolve_workspace_paths(workspace_root=tmp_path / "workspace").compiled_flow_modules_dir
    helper_modules_dir = flow_modules_dir / "flow_helpers"
    helper_modules_dir.mkdir(parents=True)
    compiled_helper_modules_dir = compiled_flow_modules_dir / "flow_helpers"
    orphan_dir = compiled_helper_modules_dir / "stale_pkg"
    orphan_dir.mkdir(parents=True)

    (helper_modules_dir / "labels.py").write_text("FLOW_LABEL = 'Updated Demo'\n", encoding="utf-8")
    (orphan_dir / "module.py").write_text("VALUE = 1\n", encoding="utf-8")

    prepare_flow_modules(data_root=tmp_path / "workspace")

    assert (compiled_helper_modules_dir / "labels.py").read_text(encoding="utf-8") == "FLOW_LABEL = 'Updated Demo'\n"
    assert orphan_dir.exists() is False

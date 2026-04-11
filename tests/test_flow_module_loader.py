from __future__ import annotations

import json

import pytest

import data_engine.flow_modules.flow_module_loader as flow_module_loader
from data_engine.authoring.flow import Flow
from data_engine.core.model import FlowValidationError
from data_engine.flow_modules.flow_module_loader import compiled_flow_module_context, current_compiled_flow_module_dir, discover_flow_module_definitions, in_compiled_flow_module_context, load_flow_module_definition


def _write_notebook(path, source_lines: list[str]) -> None:
    path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "metadata": {},
                        "source": source_lines,
                    }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )


def test_load_flow_module_definition_reads_description_and_flow_label(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    _write_notebook(
        flow_modules_dir / "demo.ipynb",
        [
            'DESCRIPTION = "Example description"\n',
            "from data_engine import Flow\n",
            "def build():\n",
            '    return Flow(name="demo", label="Demo", group="Tests").step(lambda context: context.current)\n',
        ],
    )

    definition = load_flow_module_definition("demo", data_root=workspace)
    built = definition.build()

    assert definition.description == "Example description"
    assert isinstance(built, Flow)
    assert built.label == "Demo"


def test_load_flow_module_definition_rejects_missing_build(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    _write_notebook(flow_modules_dir / "bad.ipynb", ['DESCRIPTION = "Bad"\n'])

    with pytest.raises(FlowValidationError, match="does not export a callable build"):
        load_flow_module_definition("bad", data_root=workspace)


def test_load_flow_module_definition_rejects_build_with_parameters(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    _write_notebook(
        flow_modules_dir / "bad.ipynb",
        [
            "from data_engine import Flow\n",
            "def build(flow):\n",
            '    return Flow(name="bad", group="Tests")\n',
        ],
    )

    with pytest.raises(FlowValidationError, match="must not accept any parameters"):
        load_flow_module_definition("bad", data_root=workspace)


def test_load_flow_module_definition_rejects_non_string_description(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    _write_notebook(
        flow_modules_dir / "bad.ipynb",
        [
            "DESCRIPTION = ['bad']\n",
            "from data_engine import Flow\n",
            "def build():\n",
            '    return Flow(name="bad", group="Tests").step(lambda context: context.current)\n',
        ],
    )

    with pytest.raises(FlowValidationError, match="DESCRIPTION must be a string"):
        load_flow_module_definition("bad", data_root=workspace)


def test_guarded_build_rejects_non_flow_return(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    _write_notebook(
        flow_modules_dir / "bad.ipynb",
        [
            "def build():\n",
            "    return object()\n",
        ],
    )

    definition = load_flow_module_definition("bad", data_root=workspace)
    with pytest.raises(FlowValidationError, match="did not return a Flow"):
        definition.build()


def test_discover_flow_module_definitions_skips_helper_modules(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    compiled_dir = tmp_path / "data_engine" / "artifacts" / "workspace_cache" / "workspace" / "compiled_flow_modules"
    flow_modules_dir.mkdir(parents=True)
    compiled_dir.mkdir(parents=True)
    _write_notebook(
        flow_modules_dir / "demo.ipynb",
        [
            "from data_engine import Flow\n",
            "def build():\n",
            '    return Flow(name="demo", group="Tests").step(lambda context: context.current)\n',
        ],
    )
    (compiled_dir / "_helpers.py").write_text("HELPER = True\n", encoding="utf-8")

    discovered = discover_flow_module_definitions(data_root=workspace)

    assert [item.name for item in discovered] == ["demo"]


def test_discover_flow_module_definitions_returns_empty_when_authored_flow_directory_is_missing(tmp_path):
    workspace = tmp_path / "workspace"
    compiled_dir = tmp_path / "data_engine" / "artifacts" / "workspace_cache" / "workspace" / "compiled_flow_modules"
    compiled_dir.mkdir(parents=True)
    (compiled_dir / "ghost.py").write_text(
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    discovered = discover_flow_module_definitions(data_root=workspace)

    assert discovered == ()


def test_compiled_flow_module_context_is_scoped():
    assert in_compiled_flow_module_context() is False
    assert current_compiled_flow_module_dir() is None

    with compiled_flow_module_context():
        assert in_compiled_flow_module_context() is True
        assert current_compiled_flow_module_dir() is None

    assert in_compiled_flow_module_context() is False
    assert current_compiled_flow_module_dir() is None


def test_load_flow_module_definition_resolves_relative_flow_paths_from_compiled_flow_module_dir(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    data_dir = tmp_path / "data" / "Input" / "claims_flat"
    flow_modules_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)
    _write_notebook(
        flow_modules_dir / "demo.ipynb",
        [
            "from data_engine import Flow\n",
            "def build():\n",
            "    return (\n",
            '        Flow(name="demo", group="Tests")\n',
            '        .watch(mode="poll", source="../../data/Input/claims_flat", interval="5s")\n',
            '        .mirror(root="../../data/Output/demo")\n',
            "        .step(lambda context: context.current)\n",
            "    )\n",
        ],
    )

    definition = load_flow_module_definition("demo", data_root=workspace)
    built = definition.build()

    assert built.trigger is not None
    assert built.trigger.source == (tmp_path / "data" / "Input" / "claims_flat").resolve()
    assert built.mirror_spec is not None
    assert built.mirror_spec.root == (tmp_path / "data" / "Output" / "demo").resolve()


def test_load_flow_module_definition_supports_authored_python_modules_in_flow_directory(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    (flow_modules_dir / "python_demo.py").write_text(
        'DESCRIPTION = "Authored directly in Python."\n'
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="python_demo", label="Python Demo", group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    definition = load_flow_module_definition("python_demo", data_root=workspace)
    built = definition.build()

    assert definition.description == "Authored directly in Python."
    assert isinstance(built, Flow)
    assert built.label == "Python Demo"


def test_load_flow_module_definition_supports_sibling_helper_imports_from_authored_python_modules(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    (flow_modules_dir / "helper_values.py").write_text("FLOW_LABEL = 'Helper Demo'\n", encoding="utf-8")
    (flow_modules_dir / "python_demo.py").write_text(
        "from helper_values import FLOW_LABEL\n"
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="python_demo", label=FLOW_LABEL, group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    definition = load_flow_module_definition("python_demo", data_root=workspace)
    built = definition.build()

    assert isinstance(built, Flow)
    assert built.label == "Helper Demo"


def test_load_flow_module_definition_supports_helpers_package_imports(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    helper_modules_dir = flow_modules_dir / "flow_helpers"
    helper_modules_dir.mkdir(parents=True)
    (helper_modules_dir / "labels.py").write_text("FLOW_LABEL = 'Packaged Helper Demo'\n", encoding="utf-8")
    (flow_modules_dir / "python_demo.py").write_text(
        "from flow_helpers.labels import FLOW_LABEL\n"
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="python_demo", label=FLOW_LABEL, group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    definition = load_flow_module_definition("python_demo", data_root=workspace)
    built = definition.build()

    assert isinstance(built, Flow)
    assert built.label == "Packaged Helper Demo"


def test_load_flow_module_definition_reports_import_error_details(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    (flow_modules_dir / "broken.py").write_text(
        "import does_not_exist\n"
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="broken", group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    with pytest.raises(FlowValidationError, match="does_not_exist"):
        load_flow_module_definition("broken", data_root=workspace)


def test_load_flow_module_definition_reports_missing_compiled_module_with_source_path(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    compiled_dir = tmp_path / "data_engine" / "artifacts" / "workspace_cache" / "workspace" / "compiled_flow_modules"
    flow_modules_dir.mkdir(parents=True)
    compiled_dir.mkdir(parents=True)
    (flow_modules_dir / "broken.py").write_text(
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="broken", group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(flow_module_loader, "compile_stale_flow_module_notebooks", lambda data_root=None: ())
    monkeypatch.setattr(flow_module_loader, "resolve_flow_module_paths", lambda data_root=None: (flow_modules_dir, compiled_dir))

    with pytest.raises(FlowValidationError, match="could not be compiled from"):
        load_flow_module_definition("broken", data_root=workspace)


def test_load_flow_module_definition_reports_build_failure_details(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    (flow_modules_dir / "broken.py").write_text(
        "from data_engine import Flow\n\n"
        "def build():\n"
        "    raise RuntimeError('build boom')\n",
        encoding="utf-8",
    )

    definition = load_flow_module_definition("broken", data_root=workspace)

    with pytest.raises(FlowValidationError, match='failed during build\\(\\) in build: RuntimeError: build boom'):
        definition.build()


def test_load_flow_module_definition_overrides_flow_name_with_module_name(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    (flow_modules_dir / "claims_demo.py").write_text(
        "from data_engine import Flow\n\n"
        "def build():\n"
        '    return Flow(name="broken_step", label="broken_step", group="Tests").step(lambda context: context.current)\n',
        encoding="utf-8",
    )

    definition = load_flow_module_definition("claims_demo", data_root=workspace)
    built = definition.build()

    assert built.name == "claims_demo"

from __future__ import annotations

import builtins
from concurrent.futures import ThreadPoolExecutor
import gc
import sys
from threading import Event
import weakref

import pytest

from data_engine.flow_modules.flow_module_loader import load_flow_module_definition
from data_engine.services.flow_catalog import FlowCatalogService
from data_engine.services.flow_execution import FlowExecutionService


def write_module(workspace, name, text):
    target = workspace / "flow_modules" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def test_helper_imports_work_at_build_and_execution_with_relative_packages(tmp_path):
    write_module(tmp_path, "flow_helpers/nested/__init__.py", "from .labels import LABEL\n")
    write_module(tmp_path, "flow_helpers/nested/labels.py", "LABEL = 'Deferred'\n")
    write_module(tmp_path, "demo.py", "from data_engine import Flow\n"
                 "def step(context):\n"
                 "    import flow_helpers.nested.labels\n"
                 "    return flow_helpers.nested.labels.LABEL\n"
                 "def build():\n"
                 "    from flow_helpers import nested\n"
                 "    return Flow(group='Tests', label=nested.LABEL).step(step)\n")
    flow = load_flow_module_definition("demo", data_root=tmp_path).build()
    assert flow.label == "Deferred"
    assert flow.preview() == "Deferred"


def test_annotated_dataclasses_have_module_identity_while_flow_is_owned(tmp_path):
    write_module(tmp_path, "demo.py", "from __future__ import annotations\n"
                 "from dataclasses import dataclass\nfrom data_engine import Flow\n"
                 "@dataclass\nclass Options:\n    label: str = 'Dataclass'\n"
                 "def build():\n    return Flow(group='Tests').step(lambda context: Options())\n")
    flow = load_flow_module_definition("demo", data_root=tmp_path).build()
    options = flow.preview()
    assert options.label == "Dataclass"
    assert sys.modules[type(options).__module__].Options is type(options)


def test_relative_from_import_resolves_workspace_root_and_helper_package(tmp_path):
    write_module(tmp_path, "flow_helpers/labels.py", "LABEL = 'Relative'\n")
    write_module(tmp_path, "flow_helpers/__init__.py", "from . import labels\n")
    write_module(tmp_path, "demo.py", "from data_engine import Flow\n"
                 "from . import flow_helpers\n"
                 "def step(context):\n"
                 "    from .flow_helpers import labels\n"
                 "    return labels.LABEL\n"
                 "def build():\n"
                 "    return Flow(group='Tests', label=flow_helpers.labels.LABEL).step(step)\n")
    flow = load_flow_module_definition("demo", data_root=tmp_path).build()
    assert flow.label == "Relative"
    assert flow.preview() == "Relative"


def test_invalid_import_or_definition_isolated_from_catalog_but_execution_is_strict(tmp_path):
    write_module(tmp_path, "valid.py", "from data_engine import Flow\n"
                 "def build():\n    return Flow(group='Tests').step(lambda context: 1)\n")
    write_module(tmp_path, "broken.py", "import dependency_that_does_not_exist\n")
    write_module(tmp_path, "invalid.py", "build = 123\n")
    entries = FlowCatalogService().load_entries(workspace_root=tmp_path)
    assert [(entry.name, entry.valid) for entry in entries] == [
        ("broken", False), ("invalid", False), ("valid", True),
    ]
    with pytest.raises(Exception, match="dependency_that_does_not_exist"):
        FlowExecutionService().discover_flows(workspace_root=tmp_path)


def test_overlapping_workspace_imports_are_independent(tmp_path, monkeypatch):
    a_started, b_started, a_imported = Event(), Event(), Event()
    for name, event in (("test_a_started", a_started), ("test_b_started", b_started), ("test_a_imported", a_imported)):
        monkeypatch.setattr(builtins, name, event, raising=False)
    for label in ("A", "B"):
        write_module(tmp_path / label, "flow_helpers/labels.py", f"LABEL = {label!r}\n")
    write_module(tmp_path / "A", "demo.py", "from data_engine import Flow\nimport builtins\n"
                 "builtins.test_a_started.set()\nassert builtins.test_b_started.wait(5)\n"
                 "from flow_helpers.labels import LABEL\nbuiltins.test_a_imported.set()\n"
                 "def build():\n    return Flow(group='Tests', label=LABEL).step(lambda context: LABEL)\n")
    write_module(tmp_path / "B", "demo.py", "from data_engine import Flow\nimport builtins\n"
                 "builtins.test_b_started.set()\nassert builtins.test_a_imported.wait(5)\n"
                 "from flow_helpers.labels import LABEL\n"
                 "def build():\n    return Flow(group='Tests', label=LABEL).step(lambda context: LABEL)\n")
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(load_flow_module_definition, "demo", data_root=tmp_path / "A")
        assert a_started.wait(5)
        second = pool.submit(load_flow_module_definition, "demo", data_root=tmp_path / "B")
        flows = (first.result().build(), second.result().build())
    assert [flow.preview() for flow in flows] == ["A", "B"]


def test_reload_keeps_old_helpers_consistent_and_releases_module_generations(tmp_path):
    write_module(tmp_path, "flow_helpers/labels.py", "LABEL = 'Old'\n")
    write_module(tmp_path, "demo.py", "from data_engine import Flow\n"
                 "def step(context):\n    from flow_helpers.labels import LABEL\n    return LABEL\n"
                 "def build():\n    return Flow(group='Tests').step(step)\n")
    first = load_flow_module_definition("demo", data_root=tmp_path).build()
    old_namespace = weakref.ref(first._module_namespace)
    old_prefix = first._module_namespace.prefix
    write_module(tmp_path, "flow_helpers/labels.py", "LABEL = 'New'\n")
    second = load_flow_module_definition("demo", data_root=tmp_path).build()
    assert first.preview() == "Old"
    assert second.preview() == "New"
    del first
    gc.collect()
    assert old_namespace() is None
    assert not any(name.startswith(old_prefix) for name in sys.modules)
    assert "flow_helpers" not in sys.modules

from __future__ import annotations

import json

from data_engine.devtools.project_ast_map import build_project_ast_map, main, render_project_inventory_markdown


def test_build_project_ast_map_summarizes_modules(tmp_path):
    package_root = tmp_path / "mini_pkg"
    package_root.mkdir()
    (package_root / "__init__.py").write_text('"""Mini package."""\n', encoding="utf-8")
    (package_root / "helpers.py").write_text(
        "\n".join(
            [
                "def helper():",
                "    return 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (package_root / "demo.py").write_text(
        "\n".join(
            [
                '"""Demo module."""',
                "from data_engine import Flow",
                "from .helpers import helper",
                "",
                "VALUE = 3",
                "",
                "class Demo:",
                "    KIND = 'demo'",
                "",
                "    def __init__(self, value):",
                "        self.value = value",
                "",
                "    def run(self):",
                "        return VALUE",
                "",
                "def build(context, force=False):",
                "    return Flow(name='Demo')",
                "",
                "FLOW = Flow(name='Assigned')",
                "",
            ]
        ),
        encoding="utf-8",
    )

    payload = build_project_ast_map(package_root)

    assert payload["package_root"] == "mini_pkg"
    assert payload["module_count"] == 3
    demo = next(item for item in payload["modules"] if item["module"] == "mini_pkg.demo")
    assert demo["docstring"] == "Demo module."
    assert demo["flow_calls"] == ("FLOW",)
    assert demo["functions"][0]["name"] == "build"
    assert demo["functions"][0]["params"] == ("context", "force=False")
    assert demo["classes"][0]["name"] == "Demo"
    assert demo["classes"][0]["attributes"][0]["target"] == "KIND"
    assert demo["classes"][0]["instance_attributes"] == ("value",)
    assert demo["classes"][0]["methods"][0]["name"] == "__init__"
    assert demo["classes"][0]["methods"][0]["params"] == ("self", "value")
    assert demo["assignments"][0]["target"] == "VALUE"
    assert demo["line_count"] > 0
    assert payload["import_graph"]["internal_edge_count"] == 1
    assert payload["import_graph"]["internal_edges"][0] == {
        "from": "mini_pkg.demo",
        "to": "mini_pkg.helpers",
        "kind": "internal",
    }
    assert any(item["package"] == "mini_pkg" for item in payload["package_rollups"])
    assert payload["hotspots"]["largest_modules"][0]["module"] == "mini_pkg.demo"
    assert payload["hotspots"]["most_internal_imports"][0]["module"] == "mini_pkg.demo"


def test_main_prints_json_payload(tmp_path, capsys):
    package_root = tmp_path / "mini_pkg"
    package_root.mkdir()
    (package_root / "__init__.py").write_text("", encoding="utf-8")

    result = main([str(package_root)])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["package_root"] == "mini_pkg"


def test_render_project_inventory_markdown_lists_symbols_and_params(tmp_path):
    package_root = tmp_path / "mini_pkg"
    package_root.mkdir()
    (package_root / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "demo.py").write_text(
        "\n".join(
            [
                "VALUE = 1",
                "",
                "class Demo:",
                "    KIND = 'demo'",
                "    def __init__(self, value):",
                "        self.value = value",
                "",
                "def build(context, force=False):",
                "    return context",
                "",
            ]
        ),
        encoding="utf-8",
    )

    rendered = render_project_inventory_markdown(package_root)

    assert "- module `mini_pkg.demo`" in rendered
    assert "  - attribute `VALUE`" in rendered
    assert "  - function `build`" in rendered
    assert "    - param `context`" in rendered
    assert "    - param `force=False`" in rendered
    assert "  - class `Demo`" in rendered
    assert "    - attribute `KIND`" in rendered
    assert "    - instance attribute `value`" in rendered
    assert "    - method `__init__`" in rendered

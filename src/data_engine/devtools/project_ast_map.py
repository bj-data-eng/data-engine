"""Small AST-based project mapper for the Data Engine codebase."""

from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from dataclasses import asdict, dataclass
import json
from pathlib import Path


DEFAULT_PACKAGE_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ImportSummary:
    """One import statement discovered in a module."""

    module: str | None
    names: tuple[str, ...]
    level: int = 0


@dataclass(frozen=True)
class FunctionSummary:
    """Top-level function metadata for one module."""

    name: str
    lineno: int
    params: tuple[str, ...]
    decorators: tuple[str, ...]
    returns: str | None = None
    async_def: bool = False


@dataclass(frozen=True)
class ClassSummary:
    """Top-level class metadata for one module."""

    name: str
    lineno: int
    bases: tuple[str, ...]
    attributes: tuple[AssignmentSummary, ...]
    instance_attributes: tuple[str, ...]
    methods: tuple[FunctionSummary, ...]
    decorators: tuple[str, ...]


@dataclass(frozen=True)
class AssignmentSummary:
    """Top-level assignment metadata for one module."""

    target: str
    lineno: int
    value_kind: str


@dataclass(frozen=True)
class ModuleSummary:
    """AST-level summary for one Python module."""

    module: str
    path: str
    docstring: str | None
    imports: tuple[ImportSummary, ...]
    functions: tuple[FunctionSummary, ...]
    classes: tuple[ClassSummary, ...]
    assignments: tuple[AssignmentSummary, ...]
    flow_calls: tuple[str, ...]
    line_count: int


def build_project_ast_map(package_root: Path | str | None = None) -> dict[str, object]:
    """Return an AST-derived summary of the package beneath one root."""
    root = Path(package_root or DEFAULT_PACKAGE_ROOT).resolve()
    modules = tuple(_summarize_module(root, path) for path in sorted(root.rglob("*.py")) if "__pycache__" not in path.parts)
    module_dicts = [asdict(module) for module in modules]
    import_graph = _build_import_graph(modules)
    package_rollups = _build_package_rollups(modules)
    hotspots = _build_hotspots(modules)
    return {
        "package_root": _display_package_root(root),
        "module_count": len(modules),
        "modules": module_dicts,
        "import_graph": import_graph,
        "package_rollups": package_rollups,
        "hotspots": hotspots,
    }


def render_project_inventory_markdown(package_root: Path | str | None = None) -> str:
    """Return a line-by-line Markdown inventory for one package root."""
    payload = build_project_ast_map(package_root)
    lines = [
        "# Project Inventory",
        "",
        "This page is generated from the current AST map and is intentionally inventory-shaped rather than explanatory.",
        "",
        f"- package root: `{payload['package_root']}`",
        f"- module count: `{payload['module_count']}`",
        "",
    ]
    for module in payload["modules"]:
        lines.append(f"- module `{module['module']}`")
        for assignment in module["assignments"]:
            lines.append(f"  - attribute `{assignment['target']}`")
        for function in module["functions"]:
            prefix = "async function" if function["async_def"] else "function"
            lines.append(f"  - {prefix} `{function['name']}`")
            for param in function["params"]:
                lines.append(f"    - param `{param}`")
        for class_summary in module["classes"]:
            lines.append(f"  - class `{class_summary['name']}`")
            for attribute in class_summary["attributes"]:
                lines.append(f"    - attribute `{attribute['target']}`")
            for attribute_name in class_summary["instance_attributes"]:
                lines.append(f"    - instance attribute `{attribute_name}`")
            for method in class_summary["methods"]:
                prefix = "async method" if method["async_def"] else "method"
                lines.append(f"    - {prefix} `{method['name']}`")
                for param in method["params"]:
                    lines.append(f"      - param `{param}`")
        if not module["assignments"] and not module["functions"] and not module["classes"]:
            lines.append("  - no top-level symbols")
    lines.append("")
    return "\n".join(lines)


def _summarize_module(package_root: Path, path: Path) -> ModuleSummary:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    module_name = _module_name_for_path(package_root, path)
    imports: list[ImportSummary] = []
    functions: list[FunctionSummary] = []
    classes: list[ClassSummary] = []
    assignments: list[AssignmentSummary] = []
    flow_calls: list[str] = []

    for node in tree.body:
        if isinstance(node, ast.Import):
            imports.append(
                ImportSummary(
                    module=None,
                    names=tuple(alias.name for alias in node.names),
                )
            )
            continue
        if isinstance(node, ast.ImportFrom):
            imports.append(
                ImportSummary(
                    module=node.module,
                    names=tuple(alias.name for alias in node.names),
                    level=node.level,
                )
            )
            continue
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            functions.append(_function_summary(node))
            continue
        if isinstance(node, ast.ClassDef):
            classes.append(_class_summary(node))
            continue
        if isinstance(node, ast.Assign):
            value_kind = type(node.value).__name__
            targets = [target for target in node.targets if isinstance(target, ast.Name)]
            for target in targets:
                assignments.append(
                    AssignmentSummary(
                        target=target.id,
                        lineno=node.lineno,
                        value_kind=value_kind,
                    )
                )
            if _is_flow_call(node.value):
                flow_calls.extend(target.id for target in targets)
            continue
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            assignments.append(
                AssignmentSummary(
                    target=node.target.id,
                    lineno=node.lineno,
                    value_kind=type(node.value).__name__ if node.value is not None else "None",
                )
            )
            if node.value is not None and _is_flow_call(node.value):
                flow_calls.append(node.target.id)

    return ModuleSummary(
        module=module_name,
        path=path.as_posix(),
        docstring=ast.get_docstring(tree),
        imports=tuple(imports),
        functions=tuple(functions),
        classes=tuple(classes),
        assignments=tuple(assignments),
        flow_calls=tuple(flow_calls),
        line_count=len(source.splitlines()),
    )


def _module_name_for_path(package_root: Path, path: Path) -> str:
    relative = path.relative_to(package_root)
    parts = relative.with_suffix("").parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join((package_root.name, *parts)) if parts else package_root.name


def _display_package_root(package_root: Path) -> str:
    pyproject = next((parent / "pyproject.toml" for parent in package_root.parents if (parent / "pyproject.toml").exists()), None)
    if pyproject is not None:
        try:
            return package_root.relative_to(pyproject.parent).as_posix()
        except ValueError:
            pass
    return package_root.name


def _is_flow_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == "Flow"
    return isinstance(func, ast.Attribute) and func.attr == "Flow"


def _function_summary(node: ast.FunctionDef | ast.AsyncFunctionDef) -> FunctionSummary:
    return FunctionSummary(
        name=node.name,
        lineno=node.lineno,
        params=_parameter_names(node.args),
        decorators=tuple(_expr_text(item) for item in node.decorator_list),
        returns=_expr_text(node.returns) if node.returns is not None else None,
        async_def=isinstance(node, ast.AsyncFunctionDef),
    )


def _class_summary(node: ast.ClassDef) -> ClassSummary:
    attributes: list[AssignmentSummary] = []
    methods: list[FunctionSummary] = []
    instance_attributes: list[str] = []

    for child in node.body:
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
            methods.append(_function_summary(child))
            instance_attributes.extend(_instance_attribute_names(child))
            continue
        if isinstance(child, ast.Assign):
            value_kind = type(child.value).__name__
            for target in child.targets:
                if isinstance(target, ast.Name):
                    attributes.append(
                        AssignmentSummary(
                            target=target.id,
                            lineno=child.lineno,
                            value_kind=value_kind,
                        )
                    )
            continue
        if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            attributes.append(
                AssignmentSummary(
                    target=child.target.id,
                    lineno=child.lineno,
                    value_kind=type(child.value).__name__ if child.value is not None else "None",
                )
            )

    return ClassSummary(
        name=node.name,
        lineno=node.lineno,
        bases=tuple(_expr_text(item) for item in node.bases),
        attributes=tuple(attributes),
        instance_attributes=tuple(dict.fromkeys(instance_attributes)),
        methods=tuple(methods),
        decorators=tuple(_expr_text(item) for item in node.decorator_list),
    )


def _parameter_names(args: ast.arguments) -> tuple[str, ...]:
    params: list[str] = []
    posonly_count = len(args.posonlyargs)
    combined = [*args.posonlyargs, *args.args]
    defaults = [None] * (len(combined) - len(args.defaults)) + list(args.defaults)
    for index, (arg, default) in enumerate(zip(combined, defaults, strict=False)):
        rendered = arg.arg
        if arg.annotation is not None:
            rendered = f"{rendered}: {_expr_text(arg.annotation)}"
        if default is not None:
            rendered = f"{rendered}={_expr_text(default)}"
        params.append(rendered)
        if posonly_count and index + 1 == posonly_count:
            params.append("/")
    if args.vararg is not None:
        rendered = f"*{args.vararg.arg}"
        if args.vararg.annotation is not None:
            rendered = f"{rendered}: {_expr_text(args.vararg.annotation)}"
        params.append(rendered)
    elif args.kwonlyargs:
        params.append("*")
    for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=False):
        rendered = arg.arg
        if arg.annotation is not None:
            rendered = f"{rendered}: {_expr_text(arg.annotation)}"
        if default is not None:
            rendered = f"{rendered}={_expr_text(default)}"
        params.append(rendered)
    if args.kwarg is not None:
        rendered = f"**{args.kwarg.arg}"
        if args.kwarg.annotation is not None:
            rendered = f"{rendered}: {_expr_text(args.kwarg.annotation)}"
        params.append(rendered)
    return tuple(params)


def _instance_attribute_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[str, ...]:
    names: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Assign):
            for target in child.targets:
                if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
                    names.append(target.attr)
            continue
        if isinstance(child, ast.AnnAssign):
            target = child.target
            if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
                names.append(target.attr)
    return tuple(names)


def _expr_text(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:
        return type(node).__name__


def _build_import_graph(modules: tuple[ModuleSummary, ...]) -> dict[str, object]:
    module_names = {module.module for module in modules}
    package_name = modules[0].module.split(".")[0] if modules else ""
    edges: list[dict[str, str]] = []
    internal_targets_by_source: dict[str, set[str]] = defaultdict(set)
    external_targets_by_source: dict[str, set[str]] = defaultdict(set)

    for module in modules:
        for item in module.imports:
            for target in _resolved_import_targets(module.module, item, module_names):
                if target in module_names:
                    internal_targets_by_source[module.module].add(target)
                    edges.append({"from": module.module, "to": target, "kind": "internal"})
                else:
                    external_targets_by_source[module.module].add(target)

    return {
        "internal_edges": edges,
        "internal_edge_count": len(edges),
        "external_imports": [
            {"module": module_name, "targets": sorted(targets)}
            for module_name, targets in sorted(external_targets_by_source.items())
        ],
        "fan_out": [
            {
                "module": module.module,
                "internal_targets": len(internal_targets_by_source.get(module.module, set())),
                "external_targets": len(external_targets_by_source.get(module.module, set())),
            }
            for module in modules
        ],
    }


def _build_package_rollups(modules: tuple[ModuleSummary, ...]) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for module in modules:
        package = _package_bucket(module.module)
        bucket = buckets.setdefault(
            package,
            {
                "package": package,
                "module_count": 0,
                "function_count": 0,
                "class_count": 0,
                "flow_count": 0,
                "line_count": 0,
            },
        )
        bucket["module_count"] += 1
        bucket["function_count"] += len(module.functions)
        bucket["class_count"] += len(module.classes)
        bucket["flow_count"] += len(module.flow_calls)
        bucket["line_count"] += module.line_count
    package_name = modules[0].module.split(".")[0] if modules else ""
    return sorted(buckets.values(), key=lambda item: (item["package"] != package_name, item["package"]))


def _build_hotspots(modules: tuple[ModuleSummary, ...]) -> dict[str, list[dict[str, object]]]:
    package_name = modules[0].module.split(".")[0] if modules else ""
    by_lines = sorted(
        (
            {
                "module": module.module,
                "line_count": module.line_count,
                "function_count": len(module.functions),
                "class_count": len(module.classes),
            }
            for module in modules
        ),
        key=lambda item: (-item["line_count"], item["module"]),
    )
    by_internal_fan_out = sorted(
        (
            {
                "module": module.module,
                "internal_imports": sum(
                    1
                    for item in module.imports
                    for target in _resolved_import_targets(module.module, item, set())
                    if target.startswith(package_name)
                ),
                "line_count": module.line_count,
            }
            for module in modules
        ),
        key=lambda item: (-item["internal_imports"], -item["line_count"], item["module"]),
    )
    return {
        "largest_modules": by_lines[:10],
        "most_internal_imports": by_internal_fan_out[:10],
    }


def _resolved_import_targets(module_name: str, item: ImportSummary, module_names: set[str]) -> tuple[str, ...]:
    package_name = module_name.split(".")[0]
    current_parts = module_name.split(".")
    current_package_parts = current_parts[:-1]

    if item.module is None and item.level == 0:
        return item.names

    if item.level > 0:
        anchor_parts = current_package_parts[: len(current_package_parts) - item.level + 1]
        base_parts = anchor_parts + (item.module.split(".") if item.module else [])
        base_module = ".".join(base_parts)
        return _candidate_targets(base_module, item.names, module_names)

    if item.module is None:
        return ()

    return _candidate_targets(item.module, item.names, module_names if item.module.startswith(f"{package_name}.") or item.module == package_name else set())


def _candidate_targets(base_module: str, names: tuple[str, ...], module_names: set[str]) -> tuple[str, ...]:
    if not names:
        return (base_module,) if base_module else ()
    targets: list[str] = []
    if base_module and (not module_names or base_module in module_names):
        targets.append(base_module)
    for name in names:
        if name == "*":
            continue
        candidate = ".".join(part for part in (base_module, name) if part)
        if not module_names or candidate in module_names:
            targets.append(candidate)
    if targets:
        return tuple(dict.fromkeys(targets))
    return (base_module,) if base_module else tuple(name for name in names if name != "*")


def _package_bucket(module_name: str) -> str:
    parts = module_name.split(".")
    if len(parts) <= 2:
        return module_name
    return ".".join(parts[:2])


def main(argv: list[str] | None = None) -> int:
    """Print a JSON AST project map for one package root."""
    parser = argparse.ArgumentParser(description="Build a small AST-derived map of the Data Engine project.")
    parser.add_argument(
        "package_root",
        nargs="?",
        default=str(DEFAULT_PACKAGE_ROOT),
        help="Package root to inspect. Defaults to src/data_engine.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Output format. Defaults to json.",
    )
    args = parser.parse_args(argv)
    if args.format == "markdown":
        print(render_project_inventory_markdown(args.package_root))
        return 0
    payload = build_project_ast_map(args.package_root)
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

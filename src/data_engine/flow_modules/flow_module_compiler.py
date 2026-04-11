"""Flow-module compilation and mirroring for Data Engine modules."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil

from data_engine.core.model import FlowValidationError
from data_engine.platform.workspace_models import WORKSPACE_FLOW_HELPERS_DIR_NAME
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


@dataclass(frozen=True)
class CompiledFlowModule:
    """Information about one compiled flow module."""

    name: str
    source_path: Path
    module_path: Path


def compile_stale_flow_module_notebooks(
    *,
    data_root: Path | None = None,
) -> tuple[CompiledFlowModule, ...]:
    """Compile notebook flow modules and mirror authored Python modules into compiled output."""
    flow_modules_dir, modules_dir = resolve_flow_module_paths(
        data_root=data_root,
    )
    modules_dir.mkdir(parents=True, exist_ok=True)

    if not flow_modules_dir.exists():
        return ()

    notebook_paths = sorted(flow_modules_dir.glob("*.ipynb"))
    python_paths = sorted(path for path in flow_modules_dir.glob("*.py") if path.name != "__init__.py")
    _validate_unique_authored_flow_module_stems(notebook_paths, python_paths)

    helper_modules_dir = flow_modules_dir / WORKSPACE_FLOW_HELPERS_DIR_NAME
    compiled_helper_modules_dir = modules_dir / WORKSPACE_FLOW_HELPERS_DIR_NAME
    authored_names = {path.stem for path in notebook_paths} | {path.stem for path in python_paths}
    _remove_orphaned_compiled_modules(modules_dir, authored_names)
    _mirror_helper_modules(helper_modules_dir, compiled_helper_modules_dir)

    compiled: list[CompiledFlowModule] = []
    for notebook_path in notebook_paths:
        module_path = modules_dir / f"{notebook_path.stem}.py"
        if module_path.exists() and module_path.stat().st_mtime >= notebook_path.stat().st_mtime:
            continue
        compile_flow_module_notebook(notebook_path, module_path)
        compiled.append(CompiledFlowModule(name=notebook_path.stem, source_path=notebook_path, module_path=module_path))
    for source_path in python_paths:
        module_path = modules_dir / source_path.name
        if module_path.exists() and module_path.stat().st_mtime >= source_path.stat().st_mtime:
            continue
        mirror_flow_module_python_module(source_path, module_path)
        compiled.append(CompiledFlowModule(name=source_path.stem, source_path=source_path, module_path=module_path))
    return tuple(compiled)


def resolve_flow_module_paths(
    *,
    data_root: Path | None = None,
) -> tuple[Path, Path]:
    """Resolve the authored flow-module and compiled output directories."""
    workspace = RuntimeLayoutPolicy().resolve_paths(data_root=data_root)
    return workspace.flow_modules_dir, workspace.compiled_flow_modules_dir


def compile_flow_module_notebook(notebook_path: Path, module_path: Path) -> None:
    """Compile one notebook-authored flow module into a Python module."""
    payload = json.loads(notebook_path.read_text(encoding="utf-8"))
    cells = payload.get("cells")
    if not isinstance(cells, list):
        raise FlowValidationError(f"Notebook cells payload is invalid in {notebook_path}")

    code_blocks: list[str] = []
    for cell in cells:
        if not isinstance(cell, dict) or cell.get("cell_type") != "code":
            continue
        source = cell.get("source", [])
        if isinstance(source, str):
            text = source
        elif isinstance(source, list) and all(isinstance(line, str) for line in source):
            text = "".join(source)
        else:
            raise FlowValidationError(f"Notebook code cell source is invalid in {notebook_path}")
        stripped = text.strip()
        if not stripped:
            continue
        for line in stripped.splitlines():
            if line.lstrip().startswith("%") or line.lstrip().startswith("!"):
                raise FlowValidationError(f"Notebook magics and shell commands are not allowed in {notebook_path}")
        code_blocks.append(stripped)

    if not code_blocks:
        raise FlowValidationError(f"Notebook does not contain any code cells to compile: {notebook_path}")

    rendered = [
        '"""Auto-compiled flow module. Source notebook is authoritative."""',
        "",
        "from __future__ import annotations",
        "",
        f"# Source notebook: {notebook_path.as_posix()}",
        "",
    ]
    rendered.append("\n\n".join(code_blocks))
    rendered.append("")

    module_path.parent.mkdir(parents=True, exist_ok=True)
    module_path.write_text("\n".join(rendered), encoding="utf-8")


def mirror_flow_module_python_module(source_path: Path, module_path: Path) -> None:
    """Mirror one authored Python flow/helper module into compiled output."""
    source_text = source_path.read_text(encoding="utf-8")
    rendered = [
        f"# Mirrored flow module. Source file is authoritative: {source_path.as_posix()}",
        "",
        source_text.rstrip(),
        "",
    ]
    module_path.parent.mkdir(parents=True, exist_ok=True)
    module_path.write_text("\n".join(rendered), encoding="utf-8")


def _mirror_helper_modules(helper_modules_dir: Path, compiled_helper_modules_dir: Path) -> None:
    """Mirror authored helper modules into compiled output as an importable package."""
    if compiled_helper_modules_dir.exists():
        shutil.rmtree(compiled_helper_modules_dir)
    if not helper_modules_dir.is_dir():
        return
    shutil.copytree(helper_modules_dir, compiled_helper_modules_dir)
    init_path = compiled_helper_modules_dir / "__init__.py"
    if not init_path.exists():
        init_path.write_text('"""Authored flow helper modules for flow-module imports."""\n', encoding="utf-8")


def _validate_unique_authored_flow_module_stems(notebook_paths: list[Path], python_paths: list[Path]) -> None:
    """Reject authored flow-module directories that define the same module stem twice."""
    notebook_stems = {path.stem for path in notebook_paths}
    python_stems = {path.stem for path in python_paths}
    overlaps = sorted(notebook_stems & python_stems)
    if overlaps:
        names = ", ".join(overlaps)
        raise FlowValidationError(f"Flow module sources conflict between .ipynb and .py files: {names}")


def _remove_orphaned_compiled_modules(modules_dir: Path, authored_names: set[str]) -> None:
    """Delete generated modules and caches that no longer have a notebook source."""
    for module_path in modules_dir.glob("*.py"):
        if module_path.name == "__init__.py" or module_path.stem.startswith("_"):
            continue
        if module_path.stem not in authored_names and _is_generated_module(module_path):
            module_path.unlink()

    pycache_dir = modules_dir / "__pycache__"
    if pycache_dir.exists():
        shutil.rmtree(pycache_dir)


def _is_generated_module(module_path: Path) -> bool:
    """Return whether a compiled module was generated or mirrored from authored sources."""
    try:
        first_line = module_path.read_text(encoding="utf-8").splitlines()[0]
    except (FileNotFoundError, IndexError):
        return False
    return "Auto-compiled flow module" in first_line or "Mirrored flow module" in first_line


__all__ = [
    "CompiledFlowModule",
    "compile_flow_module_notebook",
    "compile_stale_flow_module_notebooks",
    "mirror_flow_module_python_module",
    "resolve_flow_module_paths",
]

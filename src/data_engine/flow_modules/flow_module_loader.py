"""Code-native flow-module discovery and loading for Data Engine flows."""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
from contextvars import ContextVar
from importlib.abc import MetaPathFinder
from importlib.util import module_from_spec, spec_from_file_location
import inspect
from pathlib import Path
import sys
from types import ModuleType
from typing import TYPE_CHECKING, Callable

from data_engine.core.helpers import _flow_path_base_dir, _title_case_words
from data_engine.core.model import FlowExecutionError, FlowValidationError
from data_engine.flow_modules.flow_module_compiler import compile_stale_flow_module_notebooks, resolve_flow_module_paths
from data_engine.platform.workspace_models import APP_INTERNAL_ID

if TYPE_CHECKING:
    from data_engine.core.flow import Flow


_COMPILED_FLOW_MODULE_CONTEXT: ContextVar[bool] = ContextVar("compiled_flow_module_context", default=False)
_COMPILED_FLOW_MODULE_DIR: ContextVar[Path | None] = ContextVar("compiled_flow_module_dir", default=None)


@dataclass(frozen=True)
class FlowModuleDefinition:
    """Loaded flow-module callable plus optional UI metadata."""

    name: str
    description: str | None
    module_path: Path
    build: Callable[[], "Flow"]


def _load_module(name: str, *, data_root: Path | None = None):
    compile_stale_flow_module_notebooks(data_root=data_root)
    flow_modules_dir, compiled_flow_modules_dir = resolve_flow_module_paths(data_root=data_root)
    module_path = compiled_flow_modules_dir / f"{name}.py"
    if not module_path.exists():
        source_path = _authored_flow_module_source_path(name, flow_modules_dir=flow_modules_dir)
        if source_path is not None:
            raise FlowValidationError(
                f"Flow module {name!r} could not be compiled from {source_path}. No compiled module was produced."
            )
        available = _available_flow_module_names(flow_modules_dir=flow_modules_dir)
        if available:
            names_text = ", ".join(available)
            raise FlowValidationError(
                f"Flow module {name!r} is not available in {flow_modules_dir}. Available flow modules: {names_text}."
            )
        raise FlowValidationError(f"Flow module {name!r} is not available in {flow_modules_dir}.")

    module_name = f"{APP_INTERNAL_ID}_user_flow_module_{name}"
    try:
        spec = spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise FlowValidationError(f"Flow module {name!r} could not be loaded from {module_path}.")
        module = module_from_spec(spec)
        with compiled_flow_module_context(flow_modules_dir), _compiled_flow_module_import_guard(module_path.parent):
            spec.loader.exec_module(module)
    except FlowValidationError:
        raise
    except Exception as exc:
        raise FlowExecutionError(
            flow_name=name,
            phase="import",
            detail=f"{type(exc).__name__}: {exc} ({module_path})",
        ) from exc

    return module, module_path, flow_modules_dir


def _authored_flow_module_source_path(name: str, *, flow_modules_dir: Path) -> Path | None:
    """Return the authored notebook or Python source path for one flow module when present."""
    for suffix in (".py", ".ipynb"):
        source_path = flow_modules_dir / f"{name}{suffix}"
        if source_path.exists():
            return source_path
    return None


def _available_flow_module_names(*, flow_modules_dir: Path) -> tuple[str, ...]:
    """Return the authored flow-module names currently present in one workspace."""
    names = {
        path.stem
        for pattern in ("*.py", "*.ipynb")
        for path in flow_modules_dir.glob(pattern)
        if path.name != "__init__.py" and not path.stem.startswith("_")
    }
    return tuple(sorted(names))


class _WorkspaceFlowModuleFinder(MetaPathFinder):
    """Resolve workspace-local helper imports from one compiled flow-module directory."""

    def __init__(self, compiled_flow_modules_dir: Path) -> None:
        self.compiled_flow_modules_dir = compiled_flow_modules_dir
        self.local_module_names = {
            path.stem
            for path in compiled_flow_modules_dir.glob("*.py")
            if path.name != "__init__.py" and not path.stem.startswith("_")
        }

    def matches_module(self, fullname: str) -> bool:
        top_level = fullname.split(".", 1)[0]
        return top_level in self.local_module_names or fullname == "flow_helpers" or fullname.startswith("flow_helpers.")

    def find_spec(self, fullname: str, path: object = None, target: object = None):
        del path, target
        if fullname == "flow_helpers":
            package_dir = self.compiled_flow_modules_dir / "flow_helpers"
            init_path = package_dir / "__init__.py"
            if not init_path.exists():
                return None
            return spec_from_file_location(
                fullname,
                init_path,
                submodule_search_locations=[str(package_dir)],
            )
        if fullname.startswith("flow_helpers."):
            relative_name = fullname.removeprefix("flow_helpers.").replace(".", "/")
            module_path = self.compiled_flow_modules_dir / "flow_helpers" / f"{relative_name}.py"
            if module_path.exists():
                return spec_from_file_location(fullname, module_path)
            package_dir = self.compiled_flow_modules_dir / "flow_helpers" / relative_name
            init_path = package_dir / "__init__.py"
            if init_path.exists():
                return spec_from_file_location(
                    fullname,
                    init_path,
                    submodule_search_locations=[str(package_dir)],
                )
            return None
        if "." in fullname or fullname not in self.local_module_names:
            return None
        module_path = self.compiled_flow_modules_dir / f"{fullname}.py"
        if not module_path.exists():
            return None
        return spec_from_file_location(fullname, module_path)


@contextmanager
def _compiled_flow_module_import_guard(compiled_flow_modules_dir: Path):
    """Temporarily isolate workspace-local helper imports during flow-module loading."""
    finder = _WorkspaceFlowModuleFinder(compiled_flow_modules_dir)
    saved_modules: dict[str, ModuleType] = {}
    managed_names = [
        name
        for name in list(sys.modules)
        if finder.matches_module(name)
    ]
    for name in managed_names:
        module = sys.modules.pop(name, None)
        if module is not None:
            saved_modules[name] = module
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        try:
            sys.meta_path.remove(finder)
        except ValueError:
            pass
        for name in list(sys.modules):
            if finder.matches_module(name):
                sys.modules.pop(name, None)
        sys.modules.update(saved_modules)


def load_flow_module_definition(name: str, *, data_root: Path | None = None) -> FlowModuleDefinition:
    """Load one compiled flow-module definition by module name."""
    module, module_path, flow_modules_dir = _load_module(name, data_root=data_root)

    build = getattr(module, "build", None)
    if build is None or not callable(build):
        raise FlowValidationError(f"Flow module {name!r} does not export a callable build().")

    signature = inspect.signature(build)
    if len(signature.parameters) != 0:
        raise FlowValidationError(f"Flow module {name!r} build() must not accept any parameters.")

    description = getattr(module, "DESCRIPTION", None)
    if description is not None and not isinstance(description, str):
        raise FlowValidationError(f"Flow module {name!r} DESCRIPTION must be a string.")

    def guarded_build() -> "Flow":
        with compiled_flow_module_context(flow_modules_dir):
            from data_engine.core.flow import Flow

            try:
                built = build()
            except FlowValidationError:
                raise
            except Exception as exc:
                raise FlowExecutionError(
                    flow_name=name,
                    phase="build",
                    function_name=getattr(build, "__name__", "build"),
                    detail=f"{type(exc).__name__}: {exc}",
                ) from exc
        if not isinstance(built, Flow):
            raise FlowValidationError(f"Flow module {name!r} build() did not return a Flow.")
        if built.name is not None and built.name.strip() != name:
            raise FlowValidationError(
                f"Flow module {name!r} must not override the module-defined flow name. "
                "Rename the module file to change identity, or use label= for the UI title."
            )
        return built._clone(
            name=name,
            label=built.label or _title_case_words(name, empty="Flow"),
            _workspace_root=flow_modules_dir.parent.resolve(),
        )

    return FlowModuleDefinition(
        name=name,
        description=description,
        module_path=module_path,
        build=guarded_build,
    )


def discover_flow_module_definitions(*, data_root: Path | None = None) -> tuple[FlowModuleDefinition, ...]:
    """Discover and load all compiled flow-module definitions from the workspace."""
    compile_stale_flow_module_notebooks(data_root=data_root)
    flow_modules_dir, compiled_flow_modules_dir = resolve_flow_module_paths(data_root=data_root)
    if not flow_modules_dir.is_dir():
        return ()
    if not compiled_flow_modules_dir.exists():
        return ()

    discovered: list[FlowModuleDefinition] = []
    for module_path in sorted(compiled_flow_modules_dir.glob("*.py")):
        if module_path.name == "__init__.py" or module_path.stem.startswith("_"):
            continue
        discovered.append(load_flow_module_definition(module_path.stem, data_root=data_root))
    return tuple(discovered)


def in_compiled_flow_module_context() -> bool:
    """Return whether execution is currently inside a compiled flow-module context."""
    return _COMPILED_FLOW_MODULE_CONTEXT.get()


def current_compiled_flow_module_dir() -> Path | None:
    """Return the compiled flow-module directory active for the current import/build context."""
    return _COMPILED_FLOW_MODULE_DIR.get()


@contextmanager
def compiled_flow_module_context(flow_modules_dir: Path | None = None):
    """Mark the current execution context as a compiled flow-module import/build."""
    token = _COMPILED_FLOW_MODULE_CONTEXT.set(True)
    resolved_dir = flow_modules_dir.resolve() if flow_modules_dir is not None else None
    dir_token = _COMPILED_FLOW_MODULE_DIR.set(resolved_dir)
    with _flow_path_base_dir(resolved_dir):
        try:
            yield
        finally:
            _COMPILED_FLOW_MODULE_DIR.reset(dir_token)
            _COMPILED_FLOW_MODULE_CONTEXT.reset(token)


__all__ = [
    "FlowModuleDefinition",
    "compiled_flow_module_context",
    "current_compiled_flow_module_dir",
    "discover_flow_module_definitions",
    "in_compiled_flow_module_context",
    "load_flow_module_definition",
]

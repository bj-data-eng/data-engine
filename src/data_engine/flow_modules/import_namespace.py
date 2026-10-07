"""Lifetime-owned imports for one immutable generation of workspace Python code."""

from __future__ import annotations

import builtins
from importlib.util import resolve_name, spec_from_file_location
from pathlib import Path
import sys
from threading import RLock
from types import ModuleType
from uuid import uuid4
import weakref


def _unregister_modules(names: list[str]) -> None:
    for name in reversed(names):
        sys.modules.pop(name, None)


class WorkspaceImportNamespace:
    """Keep workspace imports qualified, reloadable, and owned by their flows."""

    def __init__(self, directory: Path) -> None:
        self.prefix = f"_data_engine_workspace_{uuid4().hex}"
        self._sources = {
            path.relative_to(directory).with_suffix("").as_posix().replace("/", "."): (path, path.read_bytes())
            for path in directory.rglob("*.py")
            if "__pycache__" not in path.parts
        }
        self._modules: dict[str, ModuleType] = {}
        self._lock = RLock()
        self._registered: list[str] = []
        root = ModuleType(self.prefix)
        root.__package__ = self.prefix
        root.__path__ = []
        sys.modules[self.prefix] = root
        self._registered.append(self.prefix)
        self._modules[""] = root
        weakref.finalize(self, _unregister_modules, self._registered)
        owner_ref = weakref.ref(self)
        local_roots = {name.split(".", 1)[0] for name in self._sources}

        def scoped_import(name, globals=None, locals=None, fromlist=(), level=0):
            if not level and name.split(".", 1)[0] not in local_roots:
                return builtins.__import__(name, globals, locals, fromlist, level)
            owner = owner_ref()
            if owner is None:
                raise ImportError("The workspace flow owning this import has been released.")
            local_name = name
            if level:
                qualified = resolve_name("." * level + name, globals["__package__"])
                if qualified != owner.prefix and not qualified.startswith(owner.prefix + "."):
                    return builtins.__import__(name, globals, locals, fromlist, level)
                local_name = "" if qualified == owner.prefix else qualified.removeprefix(owner.prefix + ".")
            module = owner.load(local_name)
            for item in fromlist or ():
                child_name = f"{local_name}.{item}" if local_name else item
                if item != "*" and not hasattr(module, item) and owner._contains(child_name):
                    owner.load(child_name)
            return module if fromlist else owner.load(local_name.split(".", 1)[0])

        self._builtins = dict(vars(builtins), __import__=scoped_import)

    def _contains(self, name: str) -> bool:
        return name in self._sources or f"{name}.__init__" in self._sources

    def load(self, name: str) -> ModuleType:
        """Load a local module once within this workspace generation."""
        with self._lock:
            if name in self._modules:
                return self._modules[name]
            is_package = f"{name}.__init__" in self._sources
            source = self._sources.get(f"{name}.__init__" if is_package else name)
            if source is None:
                raise ModuleNotFoundError(f"No workspace module named {name!r}", name=name)
            path, content = source
            parent_name, _, child = name.rpartition(".")
            parent = self.load(parent_name)
            qualified = f"{self.prefix}.{name}"
            module = ModuleType(qualified)
            module.__file__ = str(path)
            module.__package__ = qualified if is_package else f"{self.prefix}.{parent_name}".rstrip(".")
            module.__spec__ = spec_from_file_location(
                qualified, path, submodule_search_locations=[str(path.parent)] if is_package else None,
            )
            if is_package:
                module.__path__ = []
            module.__dict__["__builtins__"] = self._builtins
            self._modules[name] = module
            sys.modules[qualified] = module
            self._registered.append(qualified)
            setattr(parent, child, module)
            try:
                exec(compile(content, str(path), "exec"), module.__dict__)
            except BaseException:
                self._modules.pop(name, None)
                sys.modules.pop(qualified, None)
                self._registered.remove(qualified)
                if getattr(parent, child, None) is module:
                    delattr(parent, child)
                raise
            return module

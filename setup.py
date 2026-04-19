from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist


ROOT = Path(__file__).resolve().parent
DOCS_SOURCE_DIR = ROOT / "src" / "data_engine" / "docs" / "sphinx_source"
DOCS_OUTPUT_DIR = ROOT / "src" / "data_engine" / "docs" / "html"
PROJECT_AST_MAP_SCRIPT = ROOT / "src" / "data_engine" / "devtools" / "project_ast_map.py"
EGUI_PACKAGE_DIR = ROOT / "src" / "data_engine" / "ui" / "egui"


def _vendored_native_modules() -> list[Path]:
    native_modules: list[Path] = []
    for suffix in (".pyd", ".so", ".dylib"):
        native_modules.extend(EGUI_PACKAGE_DIR.glob(f"_data_engine_egui*{suffix}"))
    return sorted(native_modules)


def _build_packaged_docs() -> None:
    if not DOCS_SOURCE_DIR.is_dir():
        raise FileNotFoundError(f"Packaged docs source directory is missing: {DOCS_SOURCE_DIR}")
    subprocess.run(
        [
            sys.executable,
            str(PROJECT_AST_MAP_SCRIPT),
            str(ROOT / "src" / "data_engine"),
            "--write-docs",
            str(DOCS_SOURCE_DIR / "guides"),
        ],
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "sphinx",
            "-b",
            "html",
            str(DOCS_SOURCE_DIR),
            str(DOCS_OUTPUT_DIR),
        ],
        check=True,
    )


class build_py(_build_py):
    def run(self) -> None:
        _build_packaged_docs()
        super().run()


class sdist(_sdist):
    def run(self) -> None:
        _build_packaged_docs()
        super().run()


class bdist_wheel(_bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        if _vendored_native_modules():
            self.root_is_pure = False

    def get_tag(self) -> tuple[str, str, str]:
        python_tag, abi_tag, platform_tag = super().get_tag()
        if _vendored_native_modules():
            python_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
            abi_tag = "abi3"
        return python_tag, abi_tag, platform_tag

    def run(self) -> None:
        if sys.platform == "win32" and not _vendored_native_modules():
            raise RuntimeError(
                "Windows wheel build requires the vendored egui native module. "
                "Run scripts/build_egui_native.py first."
            )
        super().run()


setup(cmdclass={"bdist_wheel": bdist_wheel, "build_py": build_py, "sdist": sdist})

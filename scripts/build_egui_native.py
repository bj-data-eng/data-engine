"""Build and vendor the Rust egui native module into the Python package tree."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
CRATE_MANIFEST = REPO_ROOT / "rust" / "egui_surface" / "Cargo.toml"
PACKAGE_DIR = REPO_ROOT / "src" / "data_engine" / "ui" / "egui"
OUTPUT_DIR = REPO_ROOT / "dist-egui"
NATIVE_STEM = "_data_engine_egui"


def parse_args() -> argparse.Namespace:
    """Return parsed command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter used to invoke maturin.",
    )
    parser.add_argument(
        "--release",
        action="store_true",
        help="Build the native extension in release mode.",
    )
    return parser.parse_args()


def _remove_existing_native_modules() -> None:
    for suffix in (".pyd", ".so", ".dylib"):
        for path in PACKAGE_DIR.glob(f"{NATIVE_STEM}*{suffix}"):
            path.unlink()


def _build_wheel(*, python_executable: str, release: bool) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    command = [
        python_executable,
        "-m",
        "maturin",
        "build",
        "--manifest-path",
        str(CRATE_MANIFEST),
        "--out",
        str(OUTPUT_DIR),
        "--interpreter",
        python_executable,
    ]
    if release:
        command.append("--release")
    subprocess.run(command, cwd=REPO_ROOT, check=True)
    wheels = sorted(OUTPUT_DIR.glob("*.whl"), key=lambda path: path.stat().st_mtime_ns, reverse=True)
    if not wheels:
        raise RuntimeError("maturin build did not produce a wheel in dist-egui/.")
    return wheels[0]


def _extract_native_module(wheel_path: Path) -> Path:
    with zipfile.ZipFile(wheel_path) as archive:
        for member in archive.namelist():
            member_path = Path(member)
            if not member_path.name.startswith(NATIVE_STEM):
                continue
            if member_path.suffix not in {".pyd", ".so", ".dylib"}:
                continue
            target_path = PACKAGE_DIR / member_path.name
            with archive.open(member) as source, target_path.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            return target_path
    raise RuntimeError(f"No vendorable native module was found in wheel: {wheel_path}")


def main() -> int:
    """Build the Rust extension wheel and vendor its native module into src/."""
    args = parse_args()
    _remove_existing_native_modules()
    wheel_path = _build_wheel(python_executable=str(args.python), release=args.release)
    native_path = _extract_native_module(wheel_path)
    print(native_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

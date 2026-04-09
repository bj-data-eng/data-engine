"""Curated test-running commands for the CLI surface."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

from data_engine.authoring.model import FlowValidationError

TEST_SLICE_CHOICES = ("all", "unit", "ui", "qt", "tui", "integration", "live")


def checkout_tests_dir(app_root: Path) -> Path:
    """Return the repo-local tests directory for one checkout-style app root."""
    tests_dir = app_root / "tests"
    if not tests_dir.is_dir():
        raise FlowValidationError(
            f"Run tests is only available from a checkout-style app root with a tests directory: {app_root}"
        )
    return tests_dir


def raise_open_file_limit(*, minimum_soft_limit: int = 4096) -> None:
    """Best-effort raise of the soft open-file limit before long pytest runs."""
    try:
        import resource
    except ImportError:  # pragma: no cover - non-Unix fallback
        return
    try:
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (OSError, ValueError):
        return
    target_limit = hard_limit if hard_limit >= 0 else minimum_soft_limit
    target_limit = max(soft_limit, min(target_limit, minimum_soft_limit) if hard_limit >= 0 else minimum_soft_limit)
    if hard_limit >= 0:
        target_limit = min(hard_limit, max(soft_limit, minimum_soft_limit))
    if target_limit <= soft_limit:
        return
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target_limit, hard_limit))
    except (OSError, ValueError):
        return


def test_slice_args(slice_name: str, *, app_root: Path) -> tuple[str, ...]:
    tests_dir = checkout_tests_dir(app_root)
    match slice_name:
        case "all":
            return (str(tests_dir),)
        case "unit":
            return (
                str(tests_dir),
                f"--ignore={tests_dir / 'test_qt_ui.py'}",
                f"--ignore={tests_dir / 'test_tui.py'}",
                f"--ignore={tests_dir / 'test_integration.py'}",
                f"--ignore={tests_dir / 'test_live_runtime_suite.py'}",
            )
        case "ui":
            return (str(tests_dir / "test_qt_ui.py"), str(tests_dir / "test_tui.py"))
        case "qt":
            return (str(tests_dir / "test_qt_ui.py"),)
        case "tui":
            return (str(tests_dir / "test_tui.py"),)
        case "integration":
            return (str(tests_dir / "test_integration.py"),)
        case "live":
            return (str(tests_dir / "test_live_runtime_suite.py"),)
    raise FlowValidationError(f"Unknown test slice: {slice_name}")


def run_tests(*, slice_name: str, list_slices: bool, app_root: Path) -> int:
    if list_slices:
        for name in TEST_SLICE_CHOICES:
            print(name)
        return 0
    checkout_tests_dir(app_root)
    raise_open_file_limit()
    command = [sys.executable, "-m", "pytest", "-q", *test_slice_args(slice_name, app_root=app_root)]
    return subprocess.run(command, check=False).returncode

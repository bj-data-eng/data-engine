"""Check consistency between reviewed dependency pins and the runtime lock."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
import pytest


ROOT = Path(__file__).resolve().parents[1]


def _requirements(text: str) -> dict[str, Requirement]:
    return {
        canonicalize_name(requirement.name): requirement
        for value in text.splitlines()
        if (line := value.strip()) and not line.startswith("#")
        for requirement in [Requirement(line)]
    }


def test_direct_dependency_pins_match_constraints() -> None:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    constraints = _requirements(
        (ROOT / "requirements" / "constraints.txt").read_text(encoding="utf-8")
    )
    groups = [metadata["build-system"]["requires"], metadata["project"]["dependencies"]]
    groups.extend(metadata["project"]["optional-dependencies"].values())

    for group in groups:
        for value in group:
            requirement = Requirement(value)
            specifiers = list(requirement.specifier)
            assert len(specifiers) == 1
            assert specifiers[0].operator == "=="
            assert "*" not in specifiers[0].version
            assert requirement.specifier == constraints[canonicalize_name(requirement.name)].specifier


def test_runtime_lock_pins_match_constraints_and_include_hashes() -> None:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    constraints = _requirements(
        (ROOT / "requirements" / "constraints.txt").read_text(encoding="utf-8")
    )
    lock = (ROOT / "requirements" / "locked-runtime-win-py314.txt").read_text(encoding="utf-8")
    assert "--only-binary=:all:" in lock.splitlines()
    locked = {}
    for line in lock.replace("\\\n", " ").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "--")):
            continue
        value, *hashes = line.split("--hash=")
        requirement = Requirement(value.strip())
        name = canonicalize_name(requirement.name)
        assert name not in locked
        assert requirement.specifier == constraints[name].specifier
        assert hashes and all(re.fullmatch(r"sha256:[0-9a-f]{64}", item.strip()) for item in hashes)
        locked[name] = requirement

    for value in metadata["project"]["dependencies"]:
        requirement = Requirement(value)
        assert requirement.specifier == locked[canonicalize_name(requirement.name)].specifier


@pytest.mark.parametrize(
    "name",
    ["INSTALL WINDOWS.bat", "INSTALL MAC.command", "BUILD DOCS.bat", "BUILD DOCS.command"],
)
def test_installers_constrain_isolated_builds_after_pip_upgrade(name: str) -> None:
    script = (ROOT / "INSTALL" / name).read_text(encoding="utf-8")
    upgrade = script.index("--upgrade pip")
    build_constraint = script.index("PIP_BUILD_CONSTRAINT=")
    editable_install = script.index(" -e ")
    assert upgrade < build_constraint < editable_install
    assert "PIP_BUILD_CONSTRAINT=%CONSTRAINTS_FILE%" in script or (
        'PIP_BUILD_CONSTRAINT="$CONSTRAINTS_FILE"' in script
    )


def test_publish_build_constrains_tooling_and_isolated_dependencies() -> None:
    workflow = (ROOT / ".github" / "workflows" / "publish-pypi.yml").read_text(encoding="utf-8")
    assert "PIP_CONSTRAINT: ${{ github.workspace }}/requirements/constraints.txt" in workflow
    assert "PIP_BUILD_CONSTRAINT: ${{ github.workspace }}/requirements/constraints.txt" in workflow
    assert "python -m pip install --upgrade pip build" in workflow

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from data_engine.platform.paths import (
    normalized_path_text,
    path_display,
    stable_absolute_path,
    stable_path_identity_text,
    toml_path_text,
)
from data_engine.platform.workspace_models import local_workspace_namespace
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


@pytest.mark.parametrize("case_insensitive", [False, True])
@pytest.mark.parametrize(
    ("left", "right"),
    [("stra\u00dfe", "strasse"), ("caf\u00e9", "cafe\u0301"), ("\ufb00", "ff")],
)
def test_path_identity_preserves_significant_unicode(
    tmp_path, left, right, case_insensitive
):
    assert stable_path_identity_text(
        tmp_path / left, case_insensitive=case_insensitive
    ) != stable_path_identity_text(tmp_path / right, case_insensitive=case_insensitive)


def test_display_normalization_does_not_change_filesystem_text():
    value = "folder\\cafe\u0301.csv"

    assert normalized_path_text(value) == "folder/cafe\u0301.csv"
    assert toml_path_text(value) == "folder/cafe\u0301.csv"
    assert path_display(value) == "folder/caf\u00e9.csv"


@pytest.mark.skipif(sys.platform != "darwin", reason="Native macOS Unicode filesystem aliases")
def test_macos_unicode_composition_uses_filesystem_equivalence(tmp_path):
    original = tmp_path / "caf\u00e9"
    original.mkdir()
    alias = tmp_path / "cafe\u0301"
    if alias.exists() and alias.samefile(original):
        assert stable_path_identity_text(alias) == stable_path_identity_text(original)
        assert stable_path_identity_text(alias / "missing") == stable_path_identity_text(original / "missing")
    else:
        alias.mkdir()
        assert stable_path_identity_text(alias) != stable_path_identity_text(original)


def test_ascii_identity_retains_existing_case_insensitive_keys(tmp_path):
    path = tmp_path / "Workspace" / "DOCS"
    identity = stable_path_identity_text(path, case_insensitive=True)

    assert identity == str(stable_absolute_path(path)).replace("\\", "/").lower()
    assert identity == stable_path_identity_text(
        str(path).swapcase(), case_insensitive=True
    )


def test_case_sensitive_identity_keeps_ascii_case(tmp_path):
    assert stable_path_identity_text(
        tmp_path / "A", case_insensitive=False
    ) != stable_path_identity_text(tmp_path / "a", case_insensitive=False)


def test_identity_is_lexical_and_never_resolves_reparse_targets(tmp_path, monkeypatch):
    def fail_resolve(*args, **kwargs):
        pytest.fail("path identity must not dereference reparse points")

    monkeypatch.setattr(Path, "resolve", fail_resolve)
    assert stable_path_identity_text(tmp_path / "alias" / ".." / "workspace") == (
        stable_path_identity_text(tmp_path / "workspace")
    )
    assert stable_path_identity_text(tmp_path / "alias") != stable_path_identity_text(
        tmp_path / "target"
    )


@pytest.mark.skipif(os.name != "nt", reason="Native Windows filesystem comparisons")
@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("stra\u00dfe", "strasse"),
        ("caf\u00e9", "cafe\u0301"),
        ("\u212a", "k"),
        ("\u03c3", "\u03c2"),
    ],
)
def test_distinct_ntfs_roots_have_distinct_namespaces_and_endpoints(
    tmp_path, left, right
):
    roots = [tmp_path / left, tmp_path / right]
    for root in roots:
        root.mkdir()
    assert not roots[0].samefile(roots[1])

    namespaces = [local_workspace_namespace(root, "docs") for root in roots]
    endpoints = [
        RuntimeLayoutPolicy.daemon_endpoint(
            workspace_root=root,
            workspace_id="docs",
            runtime_state_dir=tmp_path / "runtime" / namespace,
        )
        for root, namespace in zip(roots, namespaces, strict=True)
    ]

    assert namespaces[0] != namespaces[1]
    assert endpoints[0] != endpoints[1]


@pytest.mark.skipif(os.name != "nt", reason="Native Windows filesystem comparisons")
@pytest.mark.parametrize(
    ("left", "right"), [("\u00e9", "\u00c9"), ("\u03c3", "\u03a3")]
)
def test_windows_unicode_case_aliases_have_one_identity(tmp_path, left, right):
    root = tmp_path / left
    root.mkdir()
    alias = tmp_path / right
    assert root.samefile(alias)
    assert stable_path_identity_text(root) == stable_path_identity_text(alias)


@pytest.mark.skipif(os.name != "nt", reason="Native Windows filesystem comparisons")
def test_windows_unicode_aliases_normalize_existing_parents_of_missing_paths(tmp_path):
    root = tmp_path / "existing-\u00e9-directory-with-long-name"
    root.mkdir()
    alias = tmp_path / "EXISTING-\u00c9-DIRECTORY-WITH-LONG-NAME"
    assert root.samefile(alias)

    assert stable_path_identity_text(root / "missing" / "input.csv") == (
        stable_path_identity_text(alias / "MISSING" / "INPUT.csv")
    )


@pytest.mark.skipif(os.name != "nt", reason="Native Windows filesystem comparisons")
def test_unicode_identity_is_stable_when_literal_path_is_created(tmp_path):
    path = tmp_path / "cafe\u0301"
    before = stable_path_identity_text(path)
    path.mkdir()
    assert stable_path_identity_text(path) == before


@pytest.mark.skipif(os.name != "nt", reason="Native Windows reparse points")
def test_windows_symlink_identity_retains_link_path(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link-\u00e9"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("creating symlinks requires developer mode or symlink privilege")

    assert link.samefile(target)
    assert stable_path_identity_text(link) != stable_path_identity_text(target)

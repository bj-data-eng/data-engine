from __future__ import annotations

import os
import subprocess

import pytest

from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy, WorkspaceDiscoveryPolicy
from data_engine.runtime.shared_state import initialize_workspace_state, resolve_workspace_bundle


@pytest.fixture(autouse=True)
def clear_workspace_selection(monkeypatch):
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ID", raising=False)
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ROOT", raising=False)


@pytest.mark.parametrize("collection_state", ["absent", "empty", "unrelated", "deleted"])
def test_explicit_collection_selection_requires_existing_workspace(tmp_path, collection_state):
    collection = tmp_path / "collection"
    policy = RuntimeLayoutPolicy()
    if collection_state != "absent":
        collection.mkdir()
    if collection_state in {"unrelated", "deleted"}:
        (collection / "alpha" / "flow_modules").mkdir(parents=True)
    if collection_state == "deleted":
        root = collection / "target"
        (root / "flow_modules").mkdir(parents=True)
        assert policy.resolve_paths(workspace_id="target", workspace_collection_root=collection).workspace_root == root
        root.rename(tmp_path / "moved_target")

    with pytest.raises(FileNotFoundError, match="target"):
        policy.resolve_paths(workspace_id="target", workspace_collection_root=collection)


def test_explicit_selection_requires_configured_collection(tmp_path, monkeypatch):
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", raising=False)

    with pytest.raises(FileNotFoundError, match="target"):
        RuntimeLayoutPolicy().resolve_paths(workspace_id="target")


def test_default_selection_can_fall_back_after_default_workspace_disappears(tmp_path, monkeypatch):
    collection = tmp_path / "collection"
    (collection / "alpha" / "flow_modules").mkdir(parents=True)
    store = LocalSettingsStore.open_default()
    store.set_default_workspace_id("deleted")
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(collection))

    paths = RuntimeLayoutPolicy().resolve_paths()

    assert paths.workspace_id == "alpha"
    assert paths.workspace_root == collection / "alpha"


def test_explicit_collection_does_not_fall_back_to_an_environment_workspace(tmp_path, monkeypatch):
    environment_root = tmp_path / "environment_workspace"
    (environment_root / "flow_modules").mkdir(parents=True)
    collection = tmp_path / "collection"
    (collection / "alpha" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_ROOT", str(environment_root))

    with pytest.raises(FileNotFoundError, match="deleted"):
        RuntimeLayoutPolicy().resolve_paths(workspace_collection_root=collection, workspace_id="deleted")
    selected = RuntimeLayoutPolicy().resolve_paths(workspace_collection_root=collection, workspace_id="alpha")
    assert selected.workspace_root == collection / "alpha"


def test_workspace_entrypoints_reopen_the_same_shared_and_runtime_state(tmp_path):
    root = tmp_path / "collection" / "docs"
    (root / "flow_modules").mkdir(parents=True)
    policy = RuntimeLayoutPolicy()
    direct = policy.resolve_paths(workspace_root=root)
    initialize_workspace_state(direct)
    bundle = resolve_workspace_bundle(direct)

    entrypoints = (
        policy.resolve_paths(workspace_collection_root=root),
        policy.resolve_paths(workspace_collection_root=root.parent, workspace_id="docs"),
        policy.resolve_paths(data_root=root),
    )
    discovery = WorkspaceDiscoveryPolicy()
    assert discovery.discover(explicit_workspace_root=root)[0].workspace_id == "docs"
    assert discovery.discover(workspace_collection_root=root)[0].workspace_id == "docs"
    for reopened in entrypoints:
        assert reopened.workspace_id == direct.workspace_id == "docs"
        assert reopened.runtime_cache_db_path == direct.runtime_cache_db_path
        assert reopened.runtime_control_db_path == direct.runtime_control_db_path
        assert reopened.daemon_endpoint_path == direct.daemon_endpoint_path
        assert resolve_workspace_bundle(reopened).root == bundle.root


def test_existing_directory_case_variants_infer_the_stored_id(tmp_path):
    root = tmp_path / "collection" / "MixedCase"
    (root / "flow_modules").mkdir(parents=True)
    variant = root.with_name("MIXEDCASE")
    if not variant.exists() or not variant.samefile(root):
        pytest.skip("Requires a case-insensitive filesystem")
    policy = RuntimeLayoutPolicy()
    original = policy.resolve_paths(workspace_root=root)
    initialize_workspace_state(original)
    bundle = resolve_workspace_bundle(original)

    for reopened in (
        policy.resolve_paths(workspace_root=variant),
        policy.resolve_paths(workspace_collection_root=variant),
        policy.resolve_paths(workspace_collection_root=root.parent, workspace_id="MIXEDCASE"),
    ):
        assert reopened.workspace_id == "MixedCase"
        assert str(reopened.workspace_root) == str(original.workspace_root)
        assert reopened.runtime_cache_db_path == original.runtime_cache_db_path
        assert reopened.daemon_endpoint_path == original.daemon_endpoint_path
        assert resolve_workspace_bundle(reopened).root == bundle.root


def test_explicit_root_alias_spelling_is_preserved(tmp_path):
    root = tmp_path / "docs"
    (root / "flow_modules").mkdir(parents=True)
    policy = RuntimeLayoutPolicy()

    for kwargs in ({"workspace_root": root}, {"workspace_collection_root": root}):
        assert policy.resolve_paths(workspace_id="My-Alias", **kwargs).workspace_id == "My-Alias"


def test_explicit_alias_allows_a_root_name_outside_the_id_length_limit(tmp_path):
    root = tmp_path / ("w" * 65)
    root.mkdir()

    assert RuntimeLayoutPolicy().resolve_paths(workspace_root=root, workspace_id="alias").workspace_id == "alias"


def test_case_distinct_workspace_roots_keep_distinct_inferred_ids(tmp_path):
    root = tmp_path / "docs"
    other = tmp_path / "DOCS"
    (root / "flow_modules").mkdir(parents=True)
    if other.exists():
        pytest.skip("Requires a case-sensitive filesystem")
    (other / "flow_modules").mkdir(parents=True)
    policy = RuntimeLayoutPolicy()

    assert policy.resolve_paths(workspace_root=root).workspace_id == "docs"
    assert policy.resolve_paths(workspace_root=other).workspace_id == "DOCS"


def test_workspace_case_variants_canonicalize_existing_parent_spelling(tmp_path):
    parent = tmp_path / "MixedParent"
    root = parent / "docs"
    (root / "flow_modules").mkdir(parents=True)
    variant = tmp_path / "MIXEDPARENT" / "DOCS"
    if not variant.exists() or not variant.samefile(root):
        pytest.skip("Requires a case-insensitive filesystem")
    policy = RuntimeLayoutPolicy()

    original = policy.resolve_paths(workspace_root=root)
    reopened = policy.resolve_paths(workspace_root=variant)
    new_target = policy.resolve_paths(workspace_root=variant.parent / "new_workspace")

    assert str(reopened.workspace_root) == str(original.workspace_root)
    assert reopened.workspace_id == original.workspace_id
    assert reopened.runtime_cache_db_path == original.runtime_cache_db_path
    assert str(new_target.workspace_root) == str(parent / "new_workspace")


@pytest.mark.skipif(os.name != "nt", reason="Windows directory junction behavior")
def test_workspace_root_canonicalization_preserves_junction_path_and_alias(tmp_path):
    target = tmp_path / "target"
    (target / "flow_modules").mkdir(parents=True)
    authored_root = tmp_path / "authored_root"
    subprocess.run(["cmd", "/c", "mklink", "/J", str(authored_root), str(target)], check=True, capture_output=True)

    paths = RuntimeLayoutPolicy().resolve_paths(workspace_root=authored_root, workspace_id="Author-Alias")

    assert paths.workspace_root.is_junction()
    assert str(paths.workspace_root) == str(authored_root)
    assert paths.workspace_id == "Author-Alias"

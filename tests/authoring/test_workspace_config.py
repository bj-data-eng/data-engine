from __future__ import annotations

import os
import subprocess

import pytest

from data_engine.authoring.flow import Flow
from data_engine.core.model import FlowValidationError
from data_engine.core.primitives import WorkspaceConfigContext
from data_engine.runtime.execution import FlowRuntime


@pytest.fixture
def config(tmp_path):
    root = tmp_path / "workspace"
    (root / "config").mkdir(parents=True)
    (root / "config" / "settings.toml").write_text(
        '[runtime]\nbatch_size = 12\ncolumns = ["a"]\n[[runtime.rules]]\nname = "original"\n', encoding="utf-8"
    )
    return WorkspaceConfigContext(root)


@pytest.mark.parametrize("reader", ["get", "require", "all"])
def test_config_nested_values_are_isolated_across_reads(config, reader):
    def read():
        return config.all()["settings"] if reader == "all" else getattr(config, reader)("settings")

    first = read()
    first["runtime"]["batch_size"] = 99
    first["runtime"]["columns"].append("b")
    first["runtime"]["rules"][0]["name"] = "mutated"
    second = read()
    second["runtime"]["columns"].clear()

    assert config.require("settings")["runtime"] == {"batch_size": 12, "columns": ["a"], "rules": [{"name": "original"}]}
    assert "batch_size = 12" in (config.config_dir / "settings.toml").read_text(encoding="utf-8")


def test_config_nested_mutation_does_not_leak_into_the_next_flow_step(config):
    def mutate(context):
        values = context.config.require("settings")
        values["runtime"]["columns"].append("mutated")
        values["runtime"]["rules"][0]["name"] = "mutated"
        return values

    flow = Flow(name="config_isolation", group="Config")._clone(_workspace_root=config.workspace_root)
    flow = flow.step(mutate).step(lambda context: context.config.require("settings"))

    context = FlowRuntime((flow,), continuous=False).run()[0]

    assert context.current["runtime"]["columns"] == ["a"]
    assert context.current["runtime"]["rules"] == [{"name": "original"}]


@pytest.mark.parametrize("name", ["", " ", ".", "..", "../outside", r"..\outside", "/outside", r"C:\outside", "C:outside", "settings\x00", "nested/settings"])
@pytest.mark.parametrize("reader", ["get", "require"])
def test_config_rejects_path_components_and_absolute_stems(config, name, reader):
    (config.workspace_root / "outside.toml").write_text("value = 42\n", encoding="utf-8")

    with pytest.raises(FlowValidationError):
        getattr(config, reader)(name)


def test_config_rejects_host_absolute_stem(config):
    outside = config.workspace_root / "outside"
    outside.with_suffix(".toml").write_text("value = 42\n", encoding="utf-8")

    with pytest.raises(FlowValidationError, match="single file stem"):
        config.require(str(outside))


def _symlink(link, target, *, directory=False):
    try:
        link.symlink_to(target, target_is_directory=directory)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"Symlinks are unavailable: {exc}")


def test_config_rejects_file_symlink_escape_and_omits_it_from_names(config):
    outside = config.workspace_root / "outside.toml"
    outside.write_text("value = 42\n", encoding="utf-8")
    _symlink(config.config_dir / "redirect.toml", outside)

    assert config.names() == ("settings",)
    with pytest.raises(FlowValidationError, match="inside"):
        config.require("redirect")
    assert tuple(config.all()) == ("settings",)


def test_config_rejects_config_directory_symlink_escape(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "settings.toml").write_text("value = 42\n", encoding="utf-8")
    _symlink(root / "config", outside, directory=True)
    config = WorkspaceConfigContext(root)

    assert config.names() == ()
    with pytest.raises(FlowValidationError, match="inside"):
        config.require("settings")


@pytest.mark.skipif(os.name != "nt", reason="Windows directory junction behavior")
def test_config_rejects_windows_directory_junction_escape(tmp_path):
    root = tmp_path / "workspace"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "settings.toml").write_text("value = 42\n", encoding="utf-8")
    subprocess.run(["cmd", "/c", "mklink", "/J", str(root / "config"), str(outside)], check=True, capture_output=True)
    config = WorkspaceConfigContext(root)

    assert (root / "config").is_junction()
    assert config.names() == ()
    with pytest.raises(FlowValidationError, match="inside"):
        config.require("settings")


def test_config_revalidates_containment_after_caching(config):
    assert config.require("settings")["runtime"]["batch_size"] == 12
    outside = config.workspace_root / "outside.toml"
    outside.write_text("value = 42\n", encoding="utf-8")
    path = config.config_dir / "settings.toml"
    path.unlink()
    _symlink(path, outside)

    with pytest.raises(FlowValidationError, match="inside"):
        config.require("settings")


def test_config_allows_contained_file_symlinks(config):
    _symlink(config.config_dir / "copy.toml", config.config_dir / "settings.toml")

    assert config.names() == ("copy", "settings")
    assert config.require("copy") == config.require("settings")


def test_config_missing_files_and_dotted_stems_keep_author_contract(config):
    (config.config_dir / "settings.dev.toml").write_text("value = 42\n", encoding="utf-8")

    assert config.get("missing") is None
    assert config.get(" settings.dev ") == {"value": 42}
    with pytest.raises(FlowValidationError, match="not found"):
        config.require("missing")
    unavailable = WorkspaceConfigContext()
    assert unavailable.names() == ()
    assert unavailable.get("settings") is None
    with pytest.raises(FlowValidationError, match="authored workspace"):
        unavailable.require("settings")

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

import data_engine.daemon_bootstrap as daemon_bootstrap

from .support import _TEST_CONTAINMENT_NONCE


@pytest.mark.parametrize("native_fault", [False, True])
def test_bootstrap_retains_fault_trace_and_uncaught_error_in_private_log(tmp_path, native_fault):
    log = tmp_path / "daemon-crash.log"
    code = """
import faulthandler, os, sys
from data_engine import daemon_bootstrap
def app(argv):
    assert '--diagnostic-log' not in argv
    assert faulthandler.is_enabled()
    if sys.argv[2] == 'True':
        if os.name == 'nt':
            import ctypes
            ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)
        else:
            import resource
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        os.abort()
    faulthandler.dump_traceback()
    raise RuntimeError('diagnostic sentinel')
daemon_bootstrap.main(
    ['--containment-nonce', 'a' * 64, '--diagnostic-log', sys.argv[1]],
    arm_watchdog_func=lambda **kwargs: None,
    verify_windows_containment_func=lambda nonce: None,
    app_main_func=app,
)
"""
    result = subprocess.run([sys.executable, "-c", code, str(log), str(native_fault)], capture_output=True, timeout=15)
    assert result.returncode != 0
    assert log.exists()
    output = log.read_text()
    assert ("Fatal Python error" if native_fault else "diagnostic sentinel") in output
    assert "Current thread" in output
    if os.name != "nt":
        assert log.stat().st_mode & 0o777 == 0o600


def test_posix_bootstrap_arms_watchdog_before_loading_daemon_app(monkeypatch):
    events = []
    arguments = [
        "--workspace",
        "/shared/default",
        "--containment-nonce",
        _TEST_CONTAINMENT_NONCE,
    ]
    monkeypatch.setattr(daemon_bootstrap, "_HOST_OS_NAME", "posix")

    result = daemon_bootstrap.main(
        arguments,
        arm_watchdog_func=lambda **kwargs: events.append(("arm", kwargs)),
        verify_windows_containment_func=lambda nonce: pytest.fail(
            "POSIX must not verify a Windows Job"
        ),
        app_main_func=lambda argv: events.append(("app", argv)) or 7,
    )

    assert result == 7
    assert events == [
        ("arm", {"containment_nonce": _TEST_CONTAINMENT_NONCE}),
        ("app", arguments),
    ]


def test_windows_bootstrap_delegates_without_posix_watchdog(monkeypatch):
    events = []
    arguments = [
        "--workspace",
        r"C:\shared\default",
        "--containment-nonce",
        _TEST_CONTAINMENT_NONCE,
    ]
    monkeypatch.setattr(daemon_bootstrap, "_HOST_OS_NAME", "nt")

    result = daemon_bootstrap.main(
        arguments,
        arm_watchdog_func=lambda **kwargs: pytest.fail("Windows must not arm POSIX containment"),
        verify_windows_containment_func=lambda nonce: events.append(("verify", nonce)),
        app_main_func=lambda argv: events.append(argv) or 3,
    )

    assert result == 3
    assert events == [("verify", _TEST_CONTAINMENT_NONCE), arguments]


def test_posix_bootstrap_refuses_missing_nonce_before_daemon_import(monkeypatch):
    monkeypatch.setattr(daemon_bootstrap, "_HOST_OS_NAME", "posix")

    with pytest.raises(SystemExit, match="requires exactly one --containment-nonce"):
        daemon_bootstrap.main(
            ["--workspace", "/shared/default"],
            arm_watchdog_func=lambda **kwargs: pytest.fail("missing nonce must not arm"),
            app_main_func=lambda argv: pytest.fail("missing nonce must not load the app"),
        )


def test_bootstrap_rejects_duplicate_containment_nonces_before_arming(monkeypatch):
    monkeypatch.setattr(daemon_bootstrap, "_HOST_OS_NAME", "posix")

    with pytest.raises(SystemExit, match="exactly one --containment-nonce"):
        daemon_bootstrap.main(
            [
                "--containment-nonce",
                _TEST_CONTAINMENT_NONCE,
                f"--containment-nonce={'cd' * 32}",
            ],
            arm_watchdog_func=lambda **kwargs: pytest.fail("ambiguous nonce must not arm"),
            app_main_func=lambda argv: pytest.fail("ambiguous nonce must not load the app"),
        )


def test_posix_launch_handshake_gates_normal_exec_after_watchdog_arm(monkeypatch):
    events = []
    arguments = [
        "--workspace",
        "/shared/default",
        "--containment-nonce",
        _TEST_CONTAINMENT_NONCE,
        "--launch-ready-fd",
        "41",
        "--launch-release-fd",
        "42",
    ]
    monkeypatch.setattr(daemon_bootstrap, "_HOST_OS_NAME", "posix")
    monkeypatch.setattr(
        daemon_bootstrap,
        "_publish_launch_identity",
        lambda fd: events.append(("publish", fd)),
    )
    monkeypatch.setattr(
        daemon_bootstrap,
        "_await_launch_release",
        lambda fd: events.append(("release", fd)),
    )
    monkeypatch.setattr(
        daemon_bootstrap.os,
        "close",
        lambda fd: events.append(("close", fd)),
    )

    with pytest.raises(RuntimeError, match="unexpectedly returned"):
        daemon_bootstrap.main(
            arguments,
            arm_watchdog_func=lambda **kwargs: events.append(("arm", kwargs))
            or type("Watchdog", (), {"pid": 99})(),
            exec_normal_stage_func=lambda argv, *, watchdog_pid: events.append(
                ("exec", argv, watchdog_pid)
            ),
            app_main_func=lambda argv: pytest.fail(
                "the isolated handshake stage must not load the daemon app"
            ),
        )

    assert events == [
        ("arm", {"containment_nonce": _TEST_CONTAINMENT_NONCE}),
        ("publish", 41),
        ("close", 41),
        ("release", 42),
        ("close", 42),
        (
            "exec",
            [
                "--workspace",
                "/shared/default",
                "--containment-nonce",
                _TEST_CONTAINMENT_NONCE,
            ],
            99,
        ),
    ]


def test_posix_normal_exec_uses_module_loader(monkeypatch):
    captured = []
    arguments = [
        "--workspace",
        "/shared/default",
        "--containment-nonce",
        _TEST_CONTAINMENT_NONCE,
    ]

    def _capture_exec(executable, argv):
        captured.append((executable, argv))
        raise OSError("exec intercepted")

    monkeypatch.setattr(daemon_bootstrap.os, "execv", _capture_exec)

    with pytest.raises(OSError, match="exec intercepted"):
        daemon_bootstrap._exec_normal_posix_stage(  # noqa: SLF001
            arguments,
            watchdog_pid=99,
        )

    assert captured == [
        (
            daemon_bootstrap.sys.executable,
            [
                daemon_bootstrap.sys.executable,
                "-P",
                "-m",
                "data_engine.daemon_bootstrap",
                "--armed-watchdog-pid",
                "99",
                *arguments,
            ],
        )
    ]


def test_posix_normal_stage_adopts_watchdog_before_loading_app(monkeypatch):
    events = []
    arguments = [
        "--armed-watchdog-pid",
        "99",
        "--workspace",
        "/shared/default",
        "--containment-nonce",
        _TEST_CONTAINMENT_NONCE,
    ]
    monkeypatch.setattr(daemon_bootstrap, "_HOST_OS_NAME", "posix")

    result = daemon_bootstrap.main(
        arguments,
        adopt_watchdog_func=lambda pid, **kwargs: events.append(
            ("adopt", pid, kwargs)
        ),
        app_main_func=lambda argv: events.append(("app", argv)) or 8,
    )

    assert result == 8
    assert events == [
        (
            "adopt",
            99,
            {"containment_nonce": _TEST_CONTAINMENT_NONCE},
        ),
        (
            "app",
            [
                "--workspace",
                "/shared/default",
                "--containment-nonce",
                _TEST_CONTAINMENT_NONCE,
            ],
        ),
    ]


def test_posix_normal_stage_preserves_interpreter_path_order(monkeypatch):
    arguments = [
        "--armed-watchdog-pid",
        "99",
        "--containment-nonce",
        _TEST_CONTAINMENT_NONCE,
    ]
    normal_interpreter_path = [
        "/flow/pythonpath",
        daemon_bootstrap._PACKAGE_ROOT,  # noqa: SLF001
        "/environment/site-packages",
    ]
    monkeypatch.setattr(daemon_bootstrap, "_HOST_OS_NAME", "posix")
    monkeypatch.setattr(
        daemon_bootstrap.sys,
        "path",
        normal_interpreter_path.copy(),
    )

    result = daemon_bootstrap.main(
        arguments,
        adopt_watchdog_func=lambda *_args, **_kwargs: None,
        app_main_func=lambda _argv: 0,
    )

    assert result == 0
    assert daemon_bootstrap.sys.path == normal_interpreter_path


def test_safe_module_launch_ignores_cwd_package_shadow(tmp_path):
    shadow_package = tmp_path / "data_engine"
    shadow_package.mkdir()
    (shadow_package / "__init__.py").write_text("", encoding="utf-8")
    (shadow_package / "daemon_bootstrap.py").write_text(
        "raise SystemExit('cwd shadow loaded')\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)

    result = subprocess.run(
        [
            daemon_bootstrap.sys.executable,
            "-P",
            "-m",
            "data_engine.daemon_bootstrap",
        ],
        cwd=Path(tmp_path),
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "cwd shadow loaded" not in result.stderr
    assert "requires exactly one --containment-nonce" in result.stderr

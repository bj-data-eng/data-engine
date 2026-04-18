from __future__ import annotations

from data_engine.ui.gui.app_binding import initial_window_size_for_screen

from tests.bootstrap.support import FakeScreen


def test_initial_window_size_for_screen_uses_screen_percentages():
    assert initial_window_size_for_screen(FakeScreen(1920, 1080)) == (1497, 907)


def test_initial_window_size_for_screen_clamps_to_screen_bounds():
    assert initial_window_size_for_screen(FakeScreen(1280, 800)) == (1180, 760)


def test_initial_window_size_for_screen_falls_back_without_geometry():
    assert initial_window_size_for_screen(None) == (1480, 920)


import ctypes
import sys
from types import SimpleNamespace

import pytest

from dbdelay.awake import ES_CONTINUOUS, ES_SYSTEM_REQUIRED, keep_awake


def test_keep_awake_sets_and_clears(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def set_state(flags: int) -> int:
        calls.append(flags)
        return 1

    fake = SimpleNamespace(kernel32=SimpleNamespace(SetThreadExecutionState=set_state))
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "windll", fake, raising=False)

    with keep_awake() as ok:
        assert ok
        assert calls == [ES_CONTINUOUS | ES_SYSTEM_REQUIRED]
    assert calls[-1] == ES_CONTINUOUS  # sleep allowed again on exit


def test_keep_awake_clears_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def set_state(flags: int) -> int:
        calls.append(flags)
        return 1

    fake = SimpleNamespace(kernel32=SimpleNamespace(SetThreadExecutionState=set_state))
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "windll", fake, raising=False)

    with pytest.raises(RuntimeError), keep_awake():
        raise RuntimeError("collector crashed")
    assert calls[-1] == ES_CONTINUOUS


def test_keep_awake_is_a_noop_elsewhere(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    with keep_awake() as ok:
        assert not ok

"""Keep Windows from sleeping while the collector runs, without changing power settings.

The request belongs to the process: it ends when the collector exits, crashes or is killed,
so the machine's normal sleep behaviour comes back on its own.
"""

from __future__ import annotations

import contextlib
import ctypes
import sys
from collections.abc import Iterator

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


@contextlib.contextmanager
def keep_awake() -> Iterator[bool]:
    """Yields True if sleep is blocked (Windows only); the screen may still turn off."""
    if sys.platform != "win32":
        yield False
        return
    kernel32 = ctypes.windll.kernel32
    # Per-thread state: set and cleared on the same (main) thread.
    ok = bool(kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED))
    try:
        yield ok
    finally:
        kernel32.SetThreadExecutionState(ES_CONTINUOUS)

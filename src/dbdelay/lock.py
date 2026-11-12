"""One collector per data directory.

An OS-level lock on a file in the data directory: a second `dbdelay collect` fails fast
instead of polling every station twice. The OS drops the lock when the process exits,
even after a crash, so there is no stale lock file to clean up.
"""

from __future__ import annotations

import contextlib
import os
import sys
from collections.abc import Iterator
from pathlib import Path

_WIN_LOCK_OFFSET = 4096


class AlreadyRunningError(RuntimeError):
    pass


@contextlib.contextmanager
def single_instance(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    f = path.open("a+", encoding="utf-8")
    try:
        if sys.platform == "win32":
            import msvcrt

            # Windows locks are mandatory: lock a byte past the PID so it stays readable.
            f.seek(_WIN_LOCK_OFFSET)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        f.close()
        raise AlreadyRunningError(
            f"another collector holds {path}; stop it before starting a new one"
        ) from exc
    try:
        f.seek(0)
        f.truncate()
        f.write(f"{os.getpid()}\n")
        f.flush()
        yield
    finally:
        f.close()

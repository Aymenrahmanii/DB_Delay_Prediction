from pathlib import Path

import pytest

from dbdelay.lock import AlreadyRunningError, single_instance


def test_second_instance_is_refused(tmp_path: Path) -> None:
    lock = tmp_path / "data" / "collector.lock"
    with single_instance(lock), pytest.raises(AlreadyRunningError), single_instance(lock):
        pass
    # Released on exit: a new collector can start.
    with single_instance(lock):
        assert lock.read_text().strip().isdigit()

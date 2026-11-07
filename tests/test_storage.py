from datetime import UTC, datetime
from pathlib import Path

from dbdelay.storage import PollLog, PollResult, RawRecord, append_raw, raw_file, read_raw


def _record(i: int, body: bytes = b"<timetable/>") -> RawRecord:
    return RawRecord("fchg", "8000105", "/fchg/8000105", f"2026-11-06T12:0{i}:00+00:00", 200, body)


def test_append_and_read_roundtrip(tmp_path: Path) -> None:
    path = raw_file(tmp_path, "fchg", "8000105", datetime(2026, 11, 6, 12, tzinfo=UTC))
    assert path.as_posix().endswith("fchg/2026-11-06/12/8000105.jsonl.gz")
    odd = "<s n='Köln'/>".encode() + b"\xff\xfe"  # non-UTF-8 bytes must survive
    for i in range(3):
        append_raw(path, _record(i))
    append_raw(path, _record(3, odd))
    records = list(read_raw(path))
    assert len(records) == 4
    assert records[3].body == odd
    assert records[0].fetched_at.endswith("12:00:00+00:00")


def test_truncated_last_member_is_skipped(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl.gz"
    append_raw(path, _record(0))
    append_raw(path, _record(1))
    data = path.read_bytes()
    path.write_bytes(data[:-10])  # simulate a crash mid-write
    records = list(read_raw(path))
    assert [r.fetched_at[-14:-6] for r in records] == ["12:00:00"]


def test_poll_log_tolerates_half_line(tmp_path: Path) -> None:
    log = PollLog(tmp_path)
    day = datetime(2026, 11, 6, tzinfo=UTC)
    log.write(PollResult("fchg", "1", "a", "b", True, 200, 1, 10, 0.1), day)
    with log.file_for(day).open("a") as f:
        f.write('{"endpoint": "fch')
    assert len(list(log.read([day]))) == 1

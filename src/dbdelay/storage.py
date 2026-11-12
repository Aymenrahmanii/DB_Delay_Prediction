"""Raw response archive and the poll log.

Raw responses are appended as JSON lines to gzip files, one file per endpoint, station and
hour: ``raw/<endpoint>/<YYYY-MM-DD>/<HH>/<eva>.jsonl.gz``. Each append is its own gzip
member, so a crash mid-write loses at most the last record and the file stays readable.
fchg/rchg files are partitioned by fetch time (UTC); plan files by the requested slot
(Berlin time), so a re-fetch of the same slot lands next to the first.

Bodies are stored as text decoded with ``surrogateescape``, which round-trips any bytes.

The poll log (``polls/<YYYY-MM-DD>.jsonl``, UTC) holds one line per scheduled poll,
successful or not; uptime and gaps are computed from it.
"""

from __future__ import annotations

import gzip
import json
import zlib
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

Endpoint = Literal["plan", "fchg", "rchg"]


@dataclass(frozen=True, slots=True)
class RawRecord:
    endpoint: Endpoint
    eva: str
    path: str
    fetched_at: str  # ISO 8601, UTC
    status: int
    body: bytes

    def to_json(self) -> str:
        return json.dumps(
            {
                "endpoint": self.endpoint,
                "eva": self.eva,
                "path": self.path,
                "fetched_at": self.fetched_at,
                "status": self.status,
                "body": self.body.decode("utf-8", errors="surrogateescape"),
            },
            ensure_ascii=True,
        )

    @classmethod
    def from_json(cls, line: str) -> RawRecord:
        d = json.loads(line)
        return cls(
            endpoint=d["endpoint"],
            eva=d["eva"],
            path=d["path"],
            fetched_at=d["fetched_at"],
            status=d["status"],
            body=d["body"].encode("utf-8", errors="surrogateescape"),
        )


def raw_file(raw_dir: Path, endpoint: Endpoint, eva: str, partition: datetime) -> Path:
    return raw_dir / endpoint / f"{partition:%Y-%m-%d}" / f"{partition:%H}" / f"{eva}.jsonl.gz"


def append_raw(path: Path, record: RawRecord) -> int:
    """Append one record as a new gzip member; returns the compressed size written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = gzip.compress((record.to_json() + "\n").encode("ascii"), compresslevel=6)
    with path.open("ab") as f:
        f.write(data)
    return len(data)


def read_raw(path: Path) -> Iterator[RawRecord]:
    """Yield every complete record; a truncated trailing member is skipped."""
    try:
        with gzip.open(path, "rt", encoding="ascii") as f:
            for line in f:
                if line.endswith("\n"):
                    yield RawRecord.from_json(line)
    except (EOFError, zlib.error, gzip.BadGzipFile):
        return


def iter_raw_files(raw_dir: Path, endpoint: Endpoint | None = None) -> Iterator[Path]:
    base = raw_dir / endpoint if endpoint else raw_dir
    yield from sorted(base.rglob("*.jsonl.gz"))


@dataclass(frozen=True, slots=True)
class PollResult:
    endpoint: Endpoint
    eva: str
    scheduled_at: str
    fetched_at: str
    ok: bool
    status: int | None
    attempts: int
    bytes: int
    elapsed_s: float
    slot: str | None = None
    error: str | None = None


class PollLog:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def file_for(self, day: datetime) -> Path:
        return self.directory / f"{day:%Y-%m-%d}.jsonl"

    def write(self, result: PollResult, when: datetime) -> None:
        path = self.file_for(when)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(result)) + "\n")

    def first_scheduled_at(self) -> datetime | None:
        """When the very first logged poll was due: the collector's first start."""
        for path in sorted(self.directory.glob("*.jsonl")):
            with path.open(encoding="utf-8") as f:
                for line in f:
                    try:
                        return datetime.fromisoformat(json.loads(line)["scheduled_at"])
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue
        return None

    def read(self, days: list[datetime]) -> Iterator[dict[str, Any]]:
        for day in days:
            path = self.file_for(day)
            if not path.exists():
                continue
            with path.open(encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue  # a half-written last line after a crash

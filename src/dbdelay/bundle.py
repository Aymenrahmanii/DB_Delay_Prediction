"""A zip of what anyone needs to repeat the final test without the raw collection.

It holds the collector's derived tables (stops, change events, observations), the poll log
(for "was the collector watching"), the archive months used for the history features, the
frozen model files with ``frozen.json``, and the published results to compare against. No
raw responses, no credentials. Making the bundle uploads nothing.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any

NOTICE = """\
DB delay prediction: data to repeat the final test.

Timetable data (c) Deutsche Bahn, CC BY 4.0, via the DB API Marketplace (Timetables API).
archive/: derived from piebro/deutsche-bahn-data (data CC BY 4.0 by Deutsche Bahn).
tables/, polls/: collected and derived by this project; models in runs/.

Repeat with:  uv run dbdelay reproduce <this zip>
"""


def make_bundle(
    data_dir: Path, frozen_file: Path, model_files: list[Path], out: Path
) -> tuple[Path, int]:
    out.parent.mkdir(parents=True, exist_ok=True)
    tables = data_dir / "tables"
    members: list[tuple[Path, str]] = [
        *(
            (f, f"data/tables/{f.relative_to(tables).as_posix()}")
            for f in sorted(tables.rglob("*.parquet"))
            if f.name != "snapshots.parquet"
        ),
        *((f, f"data/polls/{f.name}") for f in sorted((data_dir / "polls").glob("*.jsonl"))),
        *(
            (f, f"data/archive/{f.name}")
            for f in sorted((data_dir / "archive").glob("stops-*.parquet"))
        ),
        *((f, f"data/runs/{f.name}") for f in model_files),
        (frozen_file, "frozen.json"),
    ]
    results = Path("results") / "final.json"
    if results.exists():
        members.append((results, "results/final.json"))
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        z.writestr("NOTICE.txt", NOTICE)
        for src, name in members:
            z.write(src, name)
    return out, out.stat().st_size


def open_bundle(bundle: Path, into: Path) -> tuple[Path, Path, dict[str, Any] | None]:
    """Unpacks the bundle; returns its data folder, its frozen.json and the published
    results (None if the bundle was made before the final test)."""
    into.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle) as z:
        for name in z.namelist():
            target = (into / name).resolve()
            if not target.is_relative_to(into.resolve()):  # no paths outside the folder
                raise ValueError(f"unsafe path in bundle: {name}")
        z.extractall(into)
    results = into / "results" / "final.json"
    expected = json.loads(results.read_text(encoding="utf-8")) if results.exists() else None
    return into / "data", into / "frozen.json", expected

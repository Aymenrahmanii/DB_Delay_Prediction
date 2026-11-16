"""Turn the raw collector archive into Parquet tables (long-distance trains only).

``data/tables/plan_stops.parquet``
    One row per planned stop (the latest /plan fetch of it wins).
``data/tables/change_events/<YYYY-MM-DD>.parquet``
    One row each time DB's view of a stop *changed*, with the fetch time it was first seen
    (``observed_at``, UTC). Re-fetches that say the same thing are dropped, which also
    removes the duplicates of two collectors running at once. DB's forecast at any moment
    is the latest event at or before it.
``data/tables/stop_observations.parquet``
    Per stop: first and last fetch that mentioned it, and how many did.

Building needs the analysis group (pyarrow): ``uv sync --group analysis``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from dbdelay.parse import (
    PlannedStop,
    StopChange,
    parse_changes,
    parse_plan,
    split_stop_id,
)
from dbdelay.storage import RawRecord, read_raw

log = logging.getLogger(__name__)

PLAN_FILE = "plan_stops.parquet"
EVENTS_DIR = "change_events"
OBS_FILE = "stop_observations.parquet"


@dataclass
class BuildStats:
    plan_records: int = 0
    change_records: int = 0
    bad_records: int = 0
    planned_stops: int = 0
    change_events: int = 0
    repeated_fetches: int = 0
    same_instant_conflicts: int = 0
    days: list[str] = field(default_factory=list)


def _hours(raw_dir: Path, endpoints: tuple[str, ...], days: set[str] | None = None) -> list[str]:
    """``YYYY-MM-DD/HH`` partitions present for any of the endpoints, in order, optionally
    only those of the given days."""
    found: set[str] = set()
    for ep in endpoints:
        base = raw_dir / ep
        if base.is_dir():
            found.update(
                f"{d.name}/{h.name}"
                for d in base.iterdir()
                if days is None or d.name in days
                for h in d.iterdir()
            )
    return sorted(found)


def _around(days: list[str], before: int, after: int) -> set[str]:
    out: set[str] = set()
    for d in days:
        day = date.fromisoformat(d)
        out.update((day + timedelta(days=k)).isoformat() for k in range(-before, after + 1))
    return out


def _records(raw_dir: Path, endpoint: str, hour: str) -> Iterator[RawRecord]:
    for f in sorted((raw_dir / endpoint / hour).glob("*.jsonl.gz")):
        yield from read_raw(f)


def read_plan(
    raw_dir: Path, stats: BuildStats, days: set[str] | None = None
) -> dict[str, tuple[PlannedStop, str]]:
    """Long-distance planned stops by stop id, with the fetch time of the version kept."""
    plan: dict[str, tuple[PlannedStop, str]] = {}
    for hour in _hours(raw_dir, ("plan",), days):
        for rec in _records(raw_dir, "plan", hour):
            if rec.status != 200:
                continue
            stats.plan_records += 1
            try:
                stops = parse_plan(rec.body, rec.eva)
            except Exception:
                stats.bad_records += 1
                log.warning("unparseable plan %s %s", rec.path, rec.fetched_at)
                continue
            for st in stops:
                if not st.is_long_distance:
                    continue
                old = plan.get(st.stop_id)
                if old is None or rec.fetched_at >= old[1]:
                    plan[st.stop_id] = (st, rec.fetched_at)
    stats.planned_stops = len(plan)
    return plan


def _plan_table(plan: dict[str, tuple[PlannedStop, str]]) -> dict[str, list[Any]]:
    cols: dict[str, list[Any]] = defaultdict(list)
    for st, fetched in plan.values():
        sid = split_stop_id(st.stop_id)
        cols["stop_id"].append(st.stop_id)
        cols["trip_key"].append(sid.trip_key if sid else None)
        cols["ride_id"].append(sid.ride_id if sid else None)
        cols["stop_num"].append(sid.stop_num if sid else None)
        cols["eva"].append(st.eva)
        cols["train_type"].append(st.train_type)
        cols["train_number"].append(st.train_number)
        cols["line"].append(st.line)
        cols["arr_planned"].append(st.arr_planned)
        cols["dep_planned"].append(st.dep_planned)
        cols["arr_platform"].append(st.arr_platform)
        cols["dep_platform"].append(st.dep_platform)
        cols["origin"].append(st.origin)
        cols["destination"].append(st.destination)
        cols["plan_fetched_at"].append(datetime.fromisoformat(fetched))
    return cols


@dataclass
class _Seen:
    first: datetime
    last: datetime
    count: int = 0
    state: tuple[object, ...] | None = None
    event_at: datetime | None = None


def _event_row(cols: dict[str, list[Any]], ch: StopChange, at: datetime, source: str) -> None:
    cols["stop_id"].append(ch.stop_id)
    cols["eva"].append(ch.eva)
    cols["observed_at"].append(at)
    cols["source"].append(source)
    for name in (
        "arr_changed",
        "dep_changed",
        "arr_platform",
        "dep_platform",
        "arr_status",
        "dep_status",
        "arr_cancel_time",
        "dep_cancel_time",
        "train_type",
        "train_number",
        "trip_kind",
        "arr_planned",
        "dep_planned",
    ):
        cols[name].append(getattr(ch, name))


def iter_change_events(
    raw_dir: Path,
    keep: set[str],
    stats: BuildStats,
    seen: dict[str, _Seen],
    days: set[str] | None = None,
) -> Iterator[tuple[str, dict[str, list[Any]]]]:
    """Yields (UTC day, columns) per day of fetches. ``keep`` are the planned stop ids;
    stops outside it are kept only when the change marks them as a long-distance train."""
    day_cols: dict[str, list[Any]] = defaultdict(list)
    day = ""
    for hour in _hours(raw_dir, ("fchg", "rchg"), days):
        if hour[:10] != day:
            if day and day_cols:
                yield day, day_cols
            day, day_cols = hour[:10], defaultdict(list)
        batch: list[tuple[datetime, str, RawRecord]] = []
        for ep in ("fchg", "rchg"):
            for rec in _records(raw_dir, ep, hour):
                if rec.status == 200:
                    batch.append((datetime.fromisoformat(rec.fetched_at), ep, rec))
        # Changes must be applied in fetch order across both endpoints and all stations.
        batch.sort(key=lambda b: b[0])
        for at, ep, rec in batch:
            stats.change_records += 1
            try:
                changes = parse_changes(rec.body, rec.eva)
            except Exception:
                stats.bad_records += 1
                log.warning("unparseable %s %s %s", ep, rec.path, rec.fetched_at)
                continue
            for ch in changes:
                if ch.stop_id not in keep and not ch.is_long_distance:
                    continue
                s = seen.get(ch.stop_id)
                if s is None:
                    s = seen[ch.stop_id] = _Seen(first=at, last=at)
                s.last, s.count = at, s.count + 1
                state = ch.state()
                if state == s.state:
                    stats.repeated_fetches += 1
                    continue
                if at == s.event_at:
                    # Two collectors fetched at the same instant and DB answered differently;
                    # one event per stop and moment, the first fetch wins.
                    stats.same_instant_conflicts += 1
                    continue
                s.state, s.event_at = state, at
                stats.change_events += 1
                _event_row(day_cols, ch, at, ep)
    if day and day_cols:
        yield day, day_cols


def build_tables(raw_dir: Path, out_dir: Path, days: list[str] | None = None) -> BuildStats:
    """Rebuild every table from scratch (a few minutes for a week of raw data), or with
    ``days`` (UTC, ``YYYY-MM-DD``) only those days' change events, merging their plans and
    observations into the existing tables. The day before each is read too, so DB's view of
    every stop is known when the day starts and repeated states are still dropped."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    stats = BuildStats()
    out_dir.mkdir(parents=True, exist_ok=True)
    events_dir = out_dir / EVENTS_DIR
    events_dir.mkdir(exist_ok=True)
    incremental = days is not None and (out_dir / PLAN_FILE).exists()
    if not incremental:
        days = None
        for old in events_dir.glob("*.parquet"):
            old.unlink()

    # /plan partitions are Berlin slots: a day's trains are planned in the days around it.
    plan = read_plan(raw_dir, stats, _around(days, 1, 1) if days else None)
    plan_table = pa.table(_plan_table(plan), schema=_plan_schema())
    if incremental:
        plan_table = _merge(pq.read_table(out_dir / PLAN_FILE), plan_table)
    pq.write_table(plan_table, out_dir / PLAN_FILE)
    keep = set(plan_table.column("stop_id").to_pylist())

    seen: dict[str, _Seen] = {}
    obs: dict[str, list[Any]] = defaultdict(list)
    read_days = _around(days, 1, 0) if days else None
    for day, cols in iter_change_events(raw_dir, keep, stats, seen, read_days):
        if days is None or day in days:
            pq.write_table(pa.table(cols, schema=_events_schema()), events_dir / f"{day}.parquet")
            stats.days.append(day)
        # Keep memory flat: a stop not seen for a day will not come back.
        cutoff = datetime.fromisoformat(day).replace(tzinfo=UTC) - timedelta(days=1)
        for sid in [k for k, v in seen.items() if v.last < cutoff]:
            _obs_row(obs, sid, seen.pop(sid))
    for sid, s in seen.items():
        _obs_row(obs, sid, s)
    obs_table = pa.table(obs, schema=_obs_schema())
    if incremental:
        obs_table = _merge(pq.read_table(out_dir / OBS_FILE), obs_table)
    pq.write_table(obs_table, out_dir / OBS_FILE)
    return stats


def _merge(old: Any, new: Any) -> Any:
    """Rows of ``old`` whose stop id is not in ``new``, then all of ``new``."""
    import pyarrow as pa
    import pyarrow.compute as pc

    keep = pc.invert(pc.is_in(old.column("stop_id"), value_set=new.column("stop_id")))
    return pa.concat_tables([old.filter(keep).cast(new.schema), new])


def _obs_row(obs: dict[str, list[Any]], stop_id: str, s: _Seen) -> None:
    obs["stop_id"].append(stop_id)
    obs["first_seen"].append(s.first)
    obs["last_seen"].append(s.last)
    obs["n_fetches"].append(s.count)


# Schemas are functions so importing this module does not need pyarrow.
def _plan_schema() -> Any:
    import pyarrow as pa

    ts = pa.timestamp("s")
    return pa.schema(
        [
            ("stop_id", pa.string()),
            ("trip_key", pa.string()),
            ("ride_id", pa.string()),
            ("stop_num", pa.int32()),
            ("eva", pa.string()),
            ("train_type", pa.string()),
            ("train_number", pa.string()),
            ("line", pa.string()),
            ("arr_planned", ts),
            ("dep_planned", ts),
            ("arr_platform", pa.string()),
            ("dep_platform", pa.string()),
            ("origin", pa.string()),
            ("destination", pa.string()),
            ("plan_fetched_at", pa.timestamp("ms", tz="UTC")),
        ]
    )


def _events_schema() -> Any:
    import pyarrow as pa

    ts = pa.timestamp("s")
    return pa.schema(
        [
            ("stop_id", pa.string()),
            ("eva", pa.string()),
            ("observed_at", pa.timestamp("ms", tz="UTC")),
            ("source", pa.string()),
            ("arr_changed", ts),
            ("dep_changed", ts),
            ("arr_platform", pa.string()),
            ("dep_platform", pa.string()),
            ("arr_status", pa.string()),
            ("dep_status", pa.string()),
            ("arr_cancel_time", ts),
            ("dep_cancel_time", ts),
            ("train_type", pa.string()),
            ("train_number", pa.string()),
            ("trip_kind", pa.string()),
            ("arr_planned", ts),
            ("dep_planned", ts),
        ]
    )


def _obs_schema() -> Any:
    import pyarrow as pa

    utc = pa.timestamp("ms", tz="UTC")
    return pa.schema(
        [
            ("stop_id", pa.string()),
            ("first_seen", utc),
            ("last_seen", utc),
            ("n_fetches", pa.int32()),
        ]
    )

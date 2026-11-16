"""Live state and predictions from the raw files the collector is writing.

``LiveState.refresh`` reads only what was appended since the last call (it remembers how many
records of each file it has used), applies the changes in fetch order, and keeps for every
long-distance stop of the last hours its plan and DB's latest view. ``predict`` then builds,
for the moment ``now``, the same feature rows the models were trained on (``ftarget``,
``fdeparted``, ``fdb`` -> ``build_features``) and runs every model on them:

- a target is a coming collected stop of a running trip: DB expects it within 3 h, it is not
  cancelled, and the train has already left a collected stop of the trip;
- "left" means DB's departure time (changed, else planned) is past and the collector has
  polled that station in the last 10 minutes, as in the snapshots the models were scored on.

Predictions are appended to ``data/predictions/<day>.jsonl`` for nightly scoring.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from dbdelay.config import BERLIN
from dbdelay.features import build_features
from dbdelay.model import QuantileModel, feature_matrix
from dbdelay.parse import PlannedStop, StopChange, parse_changes, parse_plan, split_stop_id
from dbdelay.stations import Station
from dbdelay.stops import DIVERSION_STOP_NUM, LONG_DISTANCE
from dbdelay.storage import read_raw

log = logging.getLogger(__name__)

KEEP_HOURS = 14  # forget stops planned longer ago than this
CHANGE_LOOKBACK_HOURS = 8  # change files read at start
TARGET_MAX_MIN = 180  # predict stops DB expects within this many minutes
POLLED_WITHIN = timedelta(minutes=10)


def berlin(t: datetime) -> datetime:
    """Aware UTC to naive Berlin, the time base of every table."""
    return t.astimezone(BERLIN).replace(tzinfo=None)


@dataclass
class StopView:
    """One stop as known now; ``track`` holds DB's arrival forecast each time it changed."""

    plan: PlannedStop | None
    change: StopChange | None = None
    # (seen, arrival forecast) every time DB's arrival time changed
    track: list[tuple[datetime, datetime | None]] = field(default_factory=list)

    @property
    def arr(self) -> datetime | None:
        c = self.change.arr_changed if self.change else None
        return c or self.arr_planned

    @property
    def dep(self) -> datetime | None:
        c = self.change.dep_changed if self.change else None
        return c or self.dep_planned

    @property
    def arr_planned(self) -> datetime | None:
        if self.plan:
            return self.plan.arr_planned
        return self.change.arr_planned if self.change else None

    @property
    def dep_planned(self) -> datetime | None:
        if self.plan:
            return self.plan.dep_planned
        return self.change.dep_planned if self.change else None

    @property
    def eva(self) -> str:
        src = self.plan or self.change
        return src.eva if src else ""

    @property
    def train(self) -> tuple[str | None, str | None]:
        if self.plan:
            return self.plan.train_type, self.plan.train_number
        if self.change:
            return self.change.train_type, self.change.train_number
        return None, None

    @property
    def cancelled(self) -> bool:
        c = self.change
        return c is not None and "c" in (c.arr_status, c.dep_status)


class LiveState:
    def __init__(self, raw_dir: Path, stations: list[Station]) -> None:
        self.raw_dir = raw_dir
        self.stations = {s.eva: s for s in stations}
        self.stops: dict[str, StopView] = {}
        self.trips: dict[str, set[str]] = defaultdict(set)
        self.last_fetch: dict[str, datetime] = {}  # Berlin time of the last change fetch
        self.newest_fetch: datetime | None = None
        # file -> (size when read, records used, records in it then)
        self._used: dict[Path, tuple[int, int, int]] = {}

    # -- reading -------------------------------------------------------------------------

    def _new_records(self, path: Path, now_utc: datetime) -> list[Any]:
        """Records of ``path`` not used yet and fetched by ``now_utc``. Later ones (only in a
        replay, or a clock skew) stay for a later call; records in a file are in fetch order."""
        size = path.stat().st_size
        seen_size, used, total = self._used.get(path, (-1, 0, 0))
        if size == seen_size and used == total:
            return []
        records = list(read_raw(path))
        new = [r for r in records[used:] if datetime.fromisoformat(r.fetched_at) <= now_utc]
        self._used[path] = (size, used + len(new), len(records))
        return new

    def _files(self, hour_dirs: list[Path]) -> list[Path]:
        return [f for d in hour_dirs if d.is_dir() for f in sorted(d.glob("*.jsonl.gz"))]

    def refresh(self, now_utc: datetime) -> int:
        """Reads new raw records; returns how many change responses were applied."""
        # /plan is partitioned by the requested slot (Berlin), fchg/rchg by fetch hour (UTC).
        today = now_utc.astimezone(BERLIN)
        plan_dirs = [
            h
            for d in (today - timedelta(days=1), today, today + timedelta(days=1))
            if (base := self.raw_dir / "plan" / f"{d:%Y-%m-%d}").is_dir()
            for h in sorted(base.iterdir())
        ]
        for f in self._files(plan_dirs):
            for rec in self._new_records(f, now_utc):
                if rec.status == 200:
                    for st in parse_plan(rec.body, rec.eva):
                        if st.is_long_distance and st.train_type in LONG_DISTANCE:
                            self._add_plan(st)

        start = now_utc - timedelta(hours=CHANGE_LOOKBACK_HOURS)
        hours = [start + timedelta(hours=h) for h in range(CHANGE_LOOKBACK_HOURS + 2)]
        change_dirs = [
            self.raw_dir / ep / f"{h:%Y-%m-%d}" / f"{h:%H}"
            for ep in ("fchg", "rchg")
            for h in hours
        ]
        batch = [
            r
            for f in self._files(change_dirs)
            for r in self._new_records(f, now_utc)
            if r.status == 200
        ]
        batch.sort(key=lambda r: r.fetched_at)
        for rec in batch:
            at = berlin(datetime.fromisoformat(rec.fetched_at))
            self.last_fetch[rec.eva] = max(self.last_fetch.get(rec.eva, at), at)
            self.newest_fetch = max(self.newest_fetch or at, at)
            for ch in parse_changes(rec.body, rec.eva):
                self._apply(ch, at)
        self._prune(berlin(now_utc))
        return len(batch)

    def _add_plan(self, st: PlannedStop) -> None:
        sid = split_stop_id(st.stop_id)
        if sid is None:
            return
        view = self.stops.get(st.stop_id)
        if view is None:
            self.stops[st.stop_id] = StopView(plan=st)
        else:
            view.plan = st
        self.trips[sid.trip_key].add(st.stop_id)

    def _apply(self, ch: StopChange, at: datetime) -> None:
        view = self.stops.get(ch.stop_id)
        if view is None:
            # Long-distance stops the plan lacks: extra trains, or a /plan hour not fetched.
            if not ch.is_long_distance or ch.train_type not in LONG_DISTANCE:
                return
            sid = split_stop_id(ch.stop_id)
            if sid is None:
                return
            view = self.stops[ch.stop_id] = StopView(plan=None)
            self.trips[sid.trip_key].add(ch.stop_id)
        before = view.arr
        view.change = ch
        if not view.track or view.arr != before:
            view.track.append((at, view.arr))

    def _prune(self, now: datetime) -> None:
        cutoff = now - timedelta(hours=KEEP_HOURS)
        for trip, ids in list(self.trips.items()):
            times = [
                t
                for i in ids
                if (v := self.stops.get(i))
                for t in (v.arr_planned, v.dep_planned)
                if t
            ]
            if times and max(times) < cutoff:
                for i in ids:
                    self.stops.pop(i, None)
                del self.trips[trip]

    # -- views -----------------------------------------------------------------------------

    def trip_stops(self, trip_key: str) -> list[tuple[str, StopView]]:
        ids = self.trips.get(trip_key, set())
        rows = [(i, self.stops[i]) for i in ids if i in self.stops]
        last = datetime.max  # noqa: DTZ901 - every time here is naive Berlin
        return sorted(rows, key=lambda r: r[1].arr_planned or r[1].dep_planned or last)

    def polled(self, eva: str, now: datetime) -> bool:
        last = self.last_fetch.get(eva)
        return last is not None and now - last <= POLLED_WITHIN


@dataclass(frozen=True, slots=True)
class Prediction:
    made_at: str
    stop_id: str
    trip_key: str
    eva: str
    model: str
    pred_delay: float
    lo: float
    hi: float
    db_delay: float
    cur_delay: float
    arr_planned: str
    run_id: str = ""


def _inputs(state: LiveState, now: datetime) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    """Rows of ``ftarget`` and ``fdeparted`` for the moment ``now`` (Berlin time)."""
    targets: list[tuple[Any, ...]] = []
    departed: list[tuple[Any, ...]] = []
    for trip in list(state.trips):
        stops = state.trip_stops(trip)
        left = [
            v
            for _, v in stops
            if v.dep_planned
            and v.dep
            and v.dep <= now
            and not v.cancelled
            and state.polled(v.eva, now)
        ]
        if not left:
            continue
        for stop_id, v in stops:
            sid = split_stop_id(stop_id)
            if (
                sid is None
                or sid.stop_num >= DIVERSION_STOP_NUM
                or v.arr_planned is None
                or v.arr is None
                or v.cancelled
                or not now < v.arr <= now + timedelta(minutes=TARGET_MAX_MIN)
                or not state.polled(v.eva, now)
            ):
                continue
            prev = [w for w in left if w.dep_planned and w.dep_planned < v.arr_planned]
            if not prev:
                continue
            kind, number = v.train
            targets.append((stop_id, trip, v.eva, kind, number, v.arr_planned, None, 0, now, v.arr))
            departed += [(stop_id, 0, w.eva, w.dep_planned, w.dep) for w in prev]
    return targets, departed


def predict(
    state: LiveState,
    models: list[QuantileModel],
    stations: list[Station],
    history: list[Path],
    now_utc: datetime,
    run_id: str = "",
) -> list[Prediction]:
    now = berlin(now_utc)
    targets, departed = _inputs(state, now)
    if not targets:
        return []
    con = duckdb.connect()
    con.execute(
        """create temp table ftarget(stop_id varchar, trip_key varchar, eva varchar,
               train_type varchar, train_number varchar, arr_planned timestamp,
               actual_delay double, horizon integer, t timestamp, db_arr timestamp);
           create temp table fdeparted(stop_id varchar, horizon integer, prev_eva varchar,
               prev_planned timestamp, prev_dep timestamp);"""
    )
    con.executemany("insert into ftarget values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", targets)
    con.executemany("insert into fdeparted values (?, ?, ?, ?, ?)", departed)
    con.execute(
        """create temp table fdb as
           select stop_id, horizon, db_arr, false as db_cancelled from ftarget"""
    )
    build_features(con, stations, history, with_db=True)
    meta = {
        r[0]: r[1:]
        for r in con.execute(
            """select f.stop_id, f.trip_key, g.eva, f.arr_planned, f.db_delay, f.cur_delay
               from features f join ftarget g using (stop_id)"""
        ).fetchall()
    }
    out: list[Prediction] = []
    for m in models:
        x, _, ids = feature_matrix(con, m.features, "true")
        med, lo, hi = m.predict(x)
        for sid, a, b, c in zip(ids.get_column("stop_id").to_list(), med, lo, hi, strict=True):
            trip, eva, planned, db_delay, cur = meta[sid]
            out.append(
                Prediction(
                    made_at=now.isoformat(timespec="seconds"),
                    stop_id=sid,
                    trip_key=trip,
                    eva=eva,
                    model=m.name,
                    pred_delay=round(float(a), 2),
                    lo=round(float(b), 2),
                    hi=round(float(c), 2),
                    db_delay=round(float(db_delay), 2),
                    cur_delay=round(float(cur), 2),
                    arr_planned=planned.isoformat(timespec="minutes"),
                    run_id=run_id,
                )
            )
    return out


def log_predictions(predictions_dir: Path, preds: list[Prediction]) -> None:
    if not preds:
        return
    predictions_dir.mkdir(parents=True, exist_ok=True)
    day = preds[0].made_at[:10]
    with (predictions_dir / f"{day}.jsonl").open("a", encoding="utf-8") as f:
        for p in preds:
            f.write(json.dumps(asdict(p)) + "\n")

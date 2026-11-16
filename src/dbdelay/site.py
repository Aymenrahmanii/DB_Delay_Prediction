"""What the live site shows, without the web framework: ``Live`` holds the live state, the
models and the latest forecasts, runs a cycle (refresh, predict, log) and builds the views.
``dbdelay serve`` runs cycles on a timer; ``dbdelay replay`` runs them on a simulated clock.
"""

from __future__ import annotations

import logging
import statistics
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dbdelay.config import Settings
from dbdelay.live import LiveState, Prediction, StopView, berlin, log_predictions, predict
from dbdelay.model import QuantileModel, load_run
from dbdelay.parse import split_stop_id
from dbdelay.stations import Station, load_stations

log = logging.getLogger(__name__)

REFRESH_S = 30
TRACK_KEEP = 240  # forecasts kept per stop for the history chart
MAIN_MODEL = "with_db"


def _minutes(a: datetime | None, b: datetime | None) -> float | None:
    return (a - b).total_seconds() / 60 if a and b else None


def _hm(t: datetime | None) -> str:
    return f"{t:%H:%M}" if t else ""


@dataclass
class Live:
    """The live state, the models and the latest predictions, shared by all requests."""

    state: LiveState
    models: list[QuantileModel]
    run_id: str
    stations: list[Station]
    history: list[Path]
    predictions_dir: Path
    latest: dict[str, dict[str, Prediction]] = field(default_factory=dict)
    track: dict[str, deque[Prediction]] = field(default_factory=dict)
    last_cycle: datetime | None = None
    lags_s: deque[float] = field(default_factory=lambda: deque(maxlen=200))
    # The clock of the cycles: real time, or a replay of collected days.
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    # For scoring and model reloads (``ops.maintain``); "live" or "replay" forecasts.
    settings: Settings | None = None
    source: str = "live"
    scored_on: str = ""  # Berlin day of the last scoring pass

    def cycle(self, now_utc: datetime | None = None) -> int:
        now_utc = now_utc or self.clock()
        fresh = self.state.refresh(now_utc)
        preds = predict(self.state, self.models, self.stations, self.history, now_utc, self.run_id)
        log_predictions(self.predictions_dir, preds)
        self.latest = {}
        for p in preds:
            self.latest.setdefault(p.stop_id, {})[p.model] = p
            if p.model == MAIN_MODEL:
                self.track.setdefault(p.stop_id, deque(maxlen=TRACK_KEEP)).append(p)
        for sid in [s for s in self.track if s not in self.state.stops]:
            del self.track[sid]
        self.last_cycle = now_utc
        if fresh and self.state.newest_fetch:
            # Freshness: newest DB data in this cycle to the moment its forecasts are ready.
            done = berlin(self.clock())
            self.lags_s.append((done - self.state.newest_fetch).total_seconds())
        log.info("live: %d new change responses, %d predictions", fresh, len(preds))
        return len(preds)

    # -- views -----------------------------------------------------------------------------

    def now(self) -> datetime:
        return berlin(self.last_cycle or self.clock())

    def train(self, trip_key: str) -> dict[str, Any] | None:
        stops = self.state.trip_stops(trip_key)
        if not stops:
            return None
        now = self.now()
        rows, next_id = [], None
        for stop_id, v in stops:
            sid = split_stop_id(stop_id)
            preds = self.latest.get(stop_id, {})
            main, alt = preds.get(MAIN_MODEL), preds.get("without_db")
            passed = (v.dep or v.arr) is not None and (v.dep or v.arr) <= now  # type: ignore[operator]
            if main and next_id is None:
                next_id = stop_id
            station = self.state.stations.get(v.eva)
            rows.append(
                {
                    "stop_id": stop_id,
                    "num": sid.stop_num if sid else None,
                    "station": station.name if station else v.eva,
                    "arr_planned": _hm(v.arr_planned),
                    "dep_planned": _hm(v.dep_planned),
                    "db_time": _hm(v.arr or v.dep),
                    "db_delay": _minutes(v.arr, v.arr_planned)
                    if v.arr_planned
                    else _minutes(v.dep, v.dep_planned),
                    "pred": main.pred_delay if main else None,
                    "lo": main.lo if main else None,
                    "hi": main.hi if main else None,
                    "pred_time": _hm(v.arr_planned + timedelta(minutes=main.pred_delay))
                    if main and v.arr_planned
                    else "",
                    "pred_without_db": alt.pred_delay if alt else None,
                    "status": "cancelled"
                    if v.cancelled
                    else ("passed" if passed else ("forecast" if main else "coming")),
                }
            )
        first = stops[0][1]
        kind, number = first.train
        track = [
            {"t": p.made_at, "db": p.db_delay, "pred": p.pred_delay, "lo": p.lo, "hi": p.hi}
            for p in self.track.get(next_id or "", [])
        ]
        return {
            "trip_key": trip_key,
            "train": f"{kind or ''} {number or ''}".strip(),
            "stops": rows,
            "next": next((r for r in rows if r["stop_id"] == next_id), None),
            "track": track,
            "updated": _hm(now),
            "run_id": self.run_id,
        }

    def running(self) -> list[dict[str, Any]]:
        """Trips with at least one forecast now, the soonest coming stop first."""
        out = []
        by_trip: dict[str, list[Prediction]] = {}
        for per_model in self.latest.values():
            if p := per_model.get(MAIN_MODEL):
                by_trip.setdefault(p.trip_key, []).append(p)
        for trip, trip_preds in by_trip.items():
            stops = self.state.trip_stops(trip)
            first: StopView = stops[0][1]
            last: StopView = stops[-1][1]
            nxt = min(trip_preds, key=lambda q: q.arr_planned)
            kind, number = first.train
            st = self.state.stations.get(nxt.eva)
            out.append(
                {
                    "trip_key": trip,
                    "train": f"{kind or ''} {number or ''}".strip(),
                    "from": self._name(first.eva),
                    "to": self._name(last.eva),
                    "next": st.name if st else nxt.eva,
                    "next_planned": nxt.arr_planned[11:16],
                    "cur_delay": nxt.cur_delay,
                    "db": nxt.db_delay,
                    "pred": nxt.pred_delay,
                }
            )
        return sorted(out, key=lambda r: (r["train"].split(" ")[0], r["next_planned"]))

    def search(self, q: str) -> list[str]:
        """Trip keys of today's trips whose train matches ``q`` ("ICE 597" or "597")."""
        q = " ".join(q.upper().split())
        hits = []
        for trip in self.state.trips:
            stops = self.state.trip_stops(trip)
            if not stops:
                continue
            kind, number = stops[0][1].train
            label = f"{kind} {number}"
            if q in (label, number):
                hits.append(trip)
        return hits

    def status(self) -> dict[str, Any]:
        lags = list(self.lags_s)
        return {
            "run_id": self.run_id,
            "last_cycle": self.last_cycle.isoformat() if self.last_cycle else None,
            "updated": _hm(self.now()) if self.last_cycle else None,  # Berlin
            "newest_db_data": self.state.newest_fetch.isoformat()
            if self.state.newest_fetch
            else None,
            "stops_tracked": len(self.state.stops),
            "forecasts": sum(1 for p in self.latest.values() if MAIN_MODEL in p),
            "freshness_median_s": statistics.median(lags) if lags else None,
        }

    def _name(self, eva: str) -> str:
        st = self.state.stations.get(eva)
        return st.name if st else eva


def replay_clock(start: datetime) -> Callable[[], datetime]:
    """A clock that starts at ``start`` (aware) and runs at real speed."""
    import time

    t0 = time.monotonic()
    return lambda: start + timedelta(seconds=time.monotonic() - t0)


def build_live(
    settings: Settings, run_id: str | None = None, replay_at: datetime | None = None
) -> Live:
    stations = load_stations(settings.stations_file)
    run, models = load_run(settings.data_dir / "runs", run_id)
    return Live(
        state=LiveState(settings.raw_dir, stations),
        models=models,
        run_id=run,
        stations=stations,
        history=sorted((settings.data_dir / "archive").glob("stops-*.parquet")),
        # A replay must not mix its forecasts into the log of real ones.
        predictions_dir=settings.data_dir / ("predictions-replay" if replay_at else "predictions"),
        clock=replay_clock(replay_at) if replay_at else (lambda: datetime.now(UTC)),
        settings=settings,
        source="replay" if replay_at else "live",
    )

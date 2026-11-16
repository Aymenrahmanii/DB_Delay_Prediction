"""Operations: replaying collected days, nightly scoring and picking up new models.

The plan runs these from timers on an always-on machine. Here the collector and the site run
on demand, so the server does them itself whenever it is running (``maintain``): after
``SCORE_AFTER_HOUR`` it scores every finished day with forecasts and no score yet, and it
switches to a newer recorded model run without a restart. ``dbdelay score`` does the same
by hand.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from dbdelay import baselines as bl
from dbdelay.config import BERLIN, Settings
from dbdelay.model import latest_run_id, load_run
from dbdelay.score import DayScore, save_score, score_day
from dbdelay.site import Live, build_live
from dbdelay.stations import load_stations
from dbdelay.tables import build_tables

log = logging.getLogger(__name__)

SCORE_AFTER_HOUR = 3  # Berlin: the last trains of the day have arrived


def replay_day(
    settings: Settings,
    day: date,
    step_min: int = 5,
    start: time = time(5, 0),
    end: time = time(23, 30),
    run_id: str | None = None,
) -> int:
    """Runs live cycles over a collected day, every ``step_min`` minutes of simulated time,
    logging forecasts to ``data/predictions-replay``. Each cycle only sees data fetched by
    its moment. Returns the number of forecasts made."""
    t = datetime.combine(day, start, BERLIN).astimezone(UTC)
    stop = datetime.combine(day, end, BERLIN).astimezone(UTC)
    live = build_live(settings, run_id, replay_at=t)
    # A new replay of a day replaces the last one (and its score).
    (live.predictions_dir / f"{day.isoformat()}.jsonl").unlink(missing_ok=True)
    (settings.data_dir / "scores" / "replay" / f"{day.isoformat()}.json").unlink(missing_ok=True)
    made = 0
    while t <= stop:
        made += live.cycle(t)
        t += timedelta(minutes=step_min)
    return made


def _utc_days(day: date) -> list[str]:
    """UTC days holding the data of a Berlin day and the arrivals just after midnight."""
    return [(day + timedelta(days=k)).isoformat() for k in (-1, 0, 1)]


def unscored_days(predictions_dir: Path, scores_dir: Path, before: date) -> list[date]:
    days = []
    for f in sorted(predictions_dir.glob("*.jsonl")):
        d = date.fromisoformat(f.stem)
        if d < before and not (scores_dir / f"{d.isoformat()}.json").exists():
            days.append(d)
    return days


def _newer_raw(raw_dir: Path, events_file: Path, day: str) -> bool:
    """True if the collector wrote change data for ``day`` after its events table was built
    (or there is no table yet), so the table needs an update before scoring."""
    files = [f for ep in ("fchg", "rchg") for f in (raw_dir / ep / day).glob("*/*.jsonl.gz")]
    if not files:
        return False
    if not events_file.exists():
        return True
    return max(f.stat().st_mtime for f in files) > events_file.stat().st_mtime


def score_pending(settings: Settings, source: str, today: date) -> list[DayScore]:
    """Scores every day before ``today`` with forecasts and no score: updates the collector
    tables for that day, then scores it."""
    predictions_dir = settings.data_dir / (
        "predictions" if source == "live" else "predictions-replay"
    )
    scores_dir = settings.data_dir / "scores"
    stations = load_stations(settings.stations_file)
    paths = bl.Paths(settings.data_dir / "tables", settings.poll_log_dir)
    out = []
    for day in unscored_days(predictions_dir, scores_dir / source, today):
        stale = [
            d
            for d in _utc_days(day)
            if _newer_raw(settings.raw_dir, paths.tables_dir / "change_events" / f"{d}.parquet", d)
        ]
        if stale:
            build_tables(settings.raw_dir, paths.tables_dir, stale)
        score = score_day(paths, stations, predictions_dir / f"{day}.jsonl", day, source)
        log.info("scored %s %s: %d forecasts", source, day, score.forecasts)
        save_score(scores_dir, score)
        out.append(score)
    return out


def maintain(live: Live) -> None:
    """Called after each live cycle: nightly scoring and model reloads, when due."""
    settings = live.settings
    if settings is None:
        return
    now = live.clock().astimezone(BERLIN)
    today = now.date().isoformat()
    if now.hour >= SCORE_AFTER_HOUR and live.scored_on != today:
        score_pending(settings, live.source, now.date())
        live.scored_on = today
    runs_dir = settings.data_dir / "runs"
    latest = latest_run_id(runs_dir)
    if latest and latest != live.run_id:
        try:
            run_id, models = load_run(runs_dir, latest)
        except ValueError:  # a run without quantile models (before week 5)
            return
        log.info("switching to model run %s", run_id)
        live.run_id, live.models = run_id, models

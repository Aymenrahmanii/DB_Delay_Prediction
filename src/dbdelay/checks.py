"""Daily checks on the collector tables, so API changes and gaps show up before training.

Each check counts offending rows for the stops planned on one (Berlin) day. ``error`` checks
mean the tables are wrong; ``warn`` checks are data facts worth knowing (late polls, odd
values from DB) that do not stop the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Literal

import duckdb

from dbdelay.config import BERLIN, Settings
from dbdelay.stations import Station
from dbdelay.stops import collector_stops_sql, literal
from dbdelay.storage import PollLog
from dbdelay.tables import EVENTS_DIR


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    level: Literal["error", "warn"]
    bad: int
    total: int
    limit: float  # allowed share of bad rows

    @property
    def ok(self) -> bool:
        return self.bad <= self.limit * self.total

    def format(self) -> str:
        mark = "ok  " if self.ok else ("FAIL" if self.level == "error" else "warn")
        share = f" ({self.bad / self.total:.1%})" if self.total else ""
        return f"{mark} {self.name}: {self.bad}/{self.total}{share}"


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def check_day(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    stations: list[Station],
    tables_dir: Path,
    day: date,
) -> list[CheckResult]:
    stops = collector_stops_sql(tables_dir, stations)
    d = literal(day.isoformat())
    con.execute(
        f"""create or replace temp view day_stops as
            select * from ({stops}) s
            where cast(coalesce(dep_planned, arr_planned) as date) = date {d}"""
    )
    n = _count(con, "select count(*) from day_stops")
    trips = _count(con, "select count(distinct trip_key) from day_stops")
    ev = literal(f"{tables_dir.as_posix()}/{EVENTS_DIR}/*.parquet")
    n_ev = _count(
        con,
        f"""select count(*) from read_parquet({ev})
            where stop_id in (select stop_id from day_stops)""",
    )

    def c(
        name: str, level: Literal["error", "warn"], bad_sql: str, total: int, limit: float = 0.0
    ) -> CheckResult:
        return CheckResult(name, level, _count(con, bad_sql), total, limit)

    results = [
        c("stops planned that day", "error", f"select {int(n == 0)}", 1),
        c(
            "duplicate stop ids",
            "error",
            "select count(*) - count(distinct stop_id) from day_stops",
            n,
        ),
        c(
            "stop id without a trip key",
            "error",
            "select count(*) from day_stops where trip_key is null or stop_num is null",
            n,
        ),
        c(
            "station not in the station list",
            "error",
            "select count(*) from day_stops where station_name is null",
            n,
        ),
        c(
            "no planned arrival or departure",
            "error",
            "select count(*) from day_stops where arr_planned is null and dep_planned is null",
            n,
        ),
        c(
            "same trip twice at one station",
            "error",
            """select count(*) - count(distinct (trip_key, eva)) from day_stops
               where not is_diversion""",
            n,
        ),
        c(
            "stop order disagrees with planned times",
            "error",
            """select count(*) from (
               select coalesce(dep_planned, arr_planned) as t,
                      lag(coalesce(dep_planned, arr_planned))
                          over (partition by trip_key order by stop_num) as prev
               from day_stops where not is_diversion) where t < prev""",
            n,
        ),
        c(
            "stops of a diversion (stop number 100+)",
            "warn",
            "select count(*) from day_stops where is_diversion",
            n,
            0.02,
        ),
        c(
            "departure before arrival (planned)",
            "error",
            "select count(*) from day_stops where dep_planned < arr_planned",
            n,
        ),
        c(
            "duplicate change events",
            "error",
            f"""select count(*) - count(distinct (stop_id, observed_at))
              from read_parquet({ev}) where stop_id in (select stop_id from day_stops)""",
            n_ev,
        ),
        c(
            "delay over 12 h either way",
            "warn",
            """select count(*) from day_stops
             where abs(arr_delay_min) > 720 or abs(dep_delay_min) > 720""",
            n,
        ),
        c(
            "more than 10 min early",
            "warn",
            "select count(*) from day_stops where arr_delay_min < -10 or dep_delay_min < -10",
            n,
            0.001,
        ),
        c(
            "never seen in a change feed",
            "warn",
            "select count(*) from day_stops where not observed",
            n,
            0.05,
        ),
        c(
            "trips with one collected stop only",
            "warn",
            """select count(*) from (select trip_key from day_stops group by trip_key
                                   having count(*) = 1)""",
            trips,
            0.5,
        ),
    ]
    results.append(_poll_coverage(settings, stations, day))
    return results


def _poll_coverage(settings: Settings, stations: list[Station], day: date) -> CheckResult:
    """Hours of the (Berlin) day in which under 95 % of the fchg polls succeeded."""
    start = datetime.combine(day, time(0), BERLIN)
    hours = [start + timedelta(hours=h) for h in range(24)]
    per_hour = 3600 // settings.fchg_interval_s * len(stations)
    ok: dict[datetime, set[tuple[str, str]]] = {h: set() for h in hours}
    log = PollLog(settings.poll_log_dir)
    for row in log.read([start - timedelta(days=1), start, start + timedelta(days=1)]):
        if row["endpoint"] != "fchg" or not row["ok"]:
            continue
        t = datetime.fromisoformat(row["scheduled_at"]).astimezone(BERLIN)
        h = t.replace(minute=0, second=0, microsecond=0)
        if h in ok:
            ok[h].add((row["eva"], row["scheduled_at"]))
    bad = sum(len(v) < 0.95 * per_hour for v in ok.values())
    return CheckResult("hours with under 95 % of fchg polls", "warn", bad, 24, 0.0)

"""Ground truth, forecast snapshots and the three baselines (week 3).

**Actual time.** The API has no actual-time field. The actual arrival of a stop is DB's last
published arrival time among the changes seen no later than ``GRACE`` after the time they
announced, i.e. the last word before the train arrived. It is kept only when the collector
polled that station around then (``COVERAGE``), and cancelled stops are left out. How often
DB still moved the time afterwards is reported as a limit (``moved_after``).

**Snapshots.** For every target stop with an actual arrival and each horizon h, the moment
``t = arrival - h`` is a prediction moment, with the arrival either planned or actual
(``Basis``). Only the choice of t uses the actual time; every value is as of t, using
only data seen up to t:

- timetable: delay 0;
- persistence: the train's latest delay at a collected stop it had already left by t;
- DB: the arrival time DB published for the target stop as of t (planned if nothing yet).

A snapshot needs the collector to have polled the target station shortly before t, so "no
change seen" really means DB had published none. All three are scored on the same rows: the
rows where the train was already running (persistence exists).

Times are Berlin local, naive, as in the stop tables.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import duckdb

from dbdelay.stations import Station
from dbdelay.stops import collector_stops_sql, literal, query
from dbdelay.tables import EVENTS_DIR

HORIZONS_MIN = (15, 30, 60, 90)
# Horizon measured back from the planned arrival (known in advance) or the actual one
# (h minutes before the train really got there; how the question is usually meant).
Basis = Literal["planned", "actual"]
GRACE_MIN = 5  # changes seen up to this long after the announced time still count
COVERAGE_MIN = 10  # a successful poll of the station at most this long before the moment
MOVED_AFTER_MIN = 1  # actual and final value differing by more than this: "moved after"


@dataclass(frozen=True, slots=True)
class Paths:
    tables_dir: Path
    polls_dir: Path


def _setup(con: duckdb.DuckDBPyConnection, paths: Paths, stations: list[Station]) -> None:
    ev = literal(f"{paths.tables_dir.as_posix()}/{EVENTS_DIR}/*.parquet")
    polls = literal(f"{paths.polls_dir.as_posix()}/*.jsonl")
    con.execute(
        f"""
        create or replace temp table st as
        select * from ({collector_stops_sql(paths.tables_dir, stations)}) s
        where not is_diversion;

        create or replace temp table ev as
        select stop_id, timezone('Europe/Berlin', observed_at) as seen,
               arr_changed, dep_changed, arr_status, dep_status
        from read_parquet({ev});

        create or replace temp table polls as
        select eva, timezone('Europe/Berlin', fetched_at) as polled_at
        from read_json_auto({polls})
        where ok and endpoint in ('fchg', 'rchg') and eva is not null;
        """
    )


def _actuals(con: duckdb.DuckDBPyConnection) -> None:
    grace = f"interval {GRACE_MIN} minute"
    con.execute(
        f"""
        create or replace temp table act as
        with a as (
            select s.stop_id, e.seen,
                   coalesce(e.arr_changed, s.arr_planned) as arr_v, e.arr_status
            from st s join ev e using (stop_id)
            where s.arr_planned is not null
        ),
        last_word as (
            select stop_id,
                   arg_max(arr_v, seen) filter (where seen <= arr_v + {grace}) as arr_actual,
                   arg_max(arr_status, seen) filter (where seen <= arr_v + {grace})
                       as status_at,
                   arg_max(arr_v, seen) as arr_final
            from a group by stop_id
        ),
        joined as (
            -- A stop DB never changed arrived as planned, if the collector was watching.
            select s.stop_id, s.eva, s.trip_key, s.arr_planned,
                   coalesce(l.arr_actual, case when l.stop_id is null then s.arr_planned end)
                       as arr_actual,
                   coalesce(l.arr_final, s.arr_planned) as arr_final,
                   coalesce(l.status_at = 'c', false) or s.arr_canceled as canceled
            from st s left join last_word l using (stop_id)
            where s.arr_planned is not null
        )
        select j.*,
               p.polled_at is not null
                   and p.polled_at >= j.arr_actual - interval {COVERAGE_MIN} minute as covered,
               abs(date_diff('second', j.arr_actual, j.arr_final)) > {MOVED_AFTER_MIN * 60}
                   as moved_after
        from joined j
        asof left join polls p
            on p.eva = j.eva and p.polled_at <= j.arr_actual + {grace}
        """
    )


def _snapshots(con: duckdb.DuckDBPyConnection, horizons: tuple[int, ...], basis: Basis) -> None:
    ref = "a.arr_planned" if basis == "planned" else "a.arr_actual"
    hs = ", ".join(f"({h})" for h in horizons)
    cov = f"interval {COVERAGE_MIN} minute"
    con.execute(
        f"""
        create or replace temp table target as
        select a.stop_id, a.trip_key, a.eva, a.arr_planned, a.arr_actual, h.h as horizon,
               {ref} - to_minutes(h.h) as t
        from act a cross join (values {hs}) h(h)
        where a.covered and not a.canceled and a.arr_actual is not null;

        create or replace temp table db_fc as
        select g.stop_id, g.horizon,
               case when e.stop_id is null then g.arr_planned
                    else coalesce(e.arr_changed, g.arr_planned) end as db_arr,
               p.polled_at is not null and p.polled_at >= g.t - {cov} as covered_t
        from target g
        asof left join ev e on e.stop_id = g.stop_id and e.seen <= g.t
        asof left join polls p on p.eva = g.eva and p.polled_at <= g.t;

        -- Earlier collected stops of the same trip, as DB described them at t.
        create or replace temp table prev as
        select g.stop_id, g.horizon, g.t, s.stop_id as prev_id, s.eva as prev_eva,
               s.dep_planned as prev_planned
        from target g
        join st s on s.trip_key = g.trip_key
             and s.dep_planned is not null and s.dep_planned < g.arr_planned
             and s.stop_id <> g.stop_id;

        create or replace temp table pers as
        with v as (
            select r.stop_id, r.horizon, r.prev_planned,
                   case when e.stop_id is null then r.prev_planned
                        else coalesce(e.dep_changed, r.prev_planned) end as prev_dep,
                   p.polled_at is not null and p.polled_at >= r.t - {cov} as prev_covered, r.t
            from prev r
            asof left join ev e on e.stop_id = r.prev_id and e.seen <= r.t
            asof left join polls p on p.eva = r.prev_eva and p.polled_at <= r.t
        )
        select stop_id, horizon,
               arg_max(date_diff('second', prev_planned, prev_dep) / 60.0, prev_planned)
                   as pers_delay
        from v
        where prev_covered and prev_dep <= t  -- the train had left that stop by t
        group by stop_id, horizon;

        create or replace temp table snapshots as
        select g.stop_id, g.trip_key, g.eva, g.horizon, g.t, g.arr_planned, g.arr_actual,
               date_diff('second', g.arr_planned, g.arr_actual) / 60.0 as actual_delay,
               date_diff('second', g.arr_planned, d.db_arr) / 60.0 as db_delay,
               p.pers_delay
        from target g
        join db_fc d using (stop_id, horizon)
        left join pers p using (stop_id, horizon)
        where d.covered_t;
        """
    )


def build(
    con: duckdb.DuckDBPyConnection,
    paths: Paths,
    stations: list[Station],
    horizons: tuple[int, ...] = HORIZONS_MIN,
    basis: Basis = "actual",
) -> None:
    """Creates the temp tables ``act`` (actual times) and ``snapshots``."""
    _setup(con, paths, stations)
    _actuals(con)
    _snapshots(con, horizons, basis)


def actual_stats(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    return query(
        con,
        """select count(*) as stops,
                  count(*) filter (where canceled) as cancelled,
                  count(*) filter (where not covered or arr_actual is null) as not_covered,
                  count(*) filter (where covered and not canceled) as with_actual,
                  avg(moved_after::int) filter (where covered and not canceled)
                      as moved_after_share
           from act""",
    )[0]


def score(con: duckdb.DuckDBPyConnection, by: str = "horizon") -> list[dict[str, Any]]:
    """Mean absolute error in minutes of each baseline, on the snapshots where the train was
    already running, grouped by ``horizon`` (or ``horizon, hour``)."""
    return query(
        con,
        f"""
        select {by},
               count(*) as n,
               count(distinct stop_id) as stops,
               avg(abs(actual_delay)) as mae_timetable,
               avg(abs(actual_delay - pers_delay)) as mae_persistence,
               avg(abs(actual_delay - db_delay)) as mae_db,
               avg(actual_delay) as mean_delay
        from (select *, hour(arr_planned) as hour from snapshots)
        where pers_delay is not null
        group by all order by all
        """,
    )


def coverage(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Snapshots per horizon, and how many had a running train (persistence)."""
    return query(
        con,
        """select horizon, count(*) as snapshots,
                  count(pers_delay) as running
           from snapshots group by horizon order by horizon""",
    )

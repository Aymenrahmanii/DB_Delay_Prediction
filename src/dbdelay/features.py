"""Feature rows for the delay model, built the same way from the archive and the collector.

A row is one train, one moment t, one coming target stop: "at 14:05, ICE 597 left Kassel 8
minutes late; how late will it arrive in Frankfurt?". Both sources first produce two tables,
then ``build_features`` turns them into the same columns:

``ftarget``   stop_id, trip_key, eva, train_type, train_number, arr_planned, actual_delay,
              horizon, t
``fdeparted`` stop_id, horizon, prev_eva, prev_planned, prev_dep: collected stops of the
              trip the train had left by t, with the departure time known at t

The archive only has final values, so for training "known at t" means the final departure
time is at or before t. The collector uses what DB had published by t (``baselines``).

Leakage rules: training moments are not chosen from the outcome (see ``archive_inputs``),
the horizon (time to the *actual* arrival) is never a feature, and history features only use
days before the target day. Test moments may sit exactly h minutes before the actual
arrival: that only decides where the model is scored, not what it learned.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from dbdelay.stations import Station
from dbdelay.stops import LONG_DISTANCE, literal

FEATURES = (
    "cur_delay",  # delay leaving the last collected stop, minutes
    "delay_change",  # cur_delay minus the delay at the collected stop before it
    "n_departed",  # collected stops already left
    "min_to_planned",  # planned arrival minus t
    "naive_eta",  # minutes until arrival if the current delay stays
    "min_since_dep",  # t minus the departure from the last collected stop
    "planned_run",  # planned minutes from that departure to the target arrival
    "hour",
    "dow",
    "eva_code",
    "prev_eva_code",
    "type_code",
    "hist_median",  # this train's arrival delay at this station, previous 28 days
    "hist_p90",
    "hist_n",
)
CATEGORICAL = ("eva_code", "prev_eva_code", "type_code", "dow")
HISTORY_DAYS = 28
GRID_MIN = 15  # training moments every 15 minutes
LOOKBACK_MIN = 120  # from 2 h before the planned arrival
LOOKAHEAD_MIN = 180  # to at most 3 h after it (or the actual arrival, if earlier)


def _list(values: list[str] | tuple[str, ...]) -> str:
    return "[" + ", ".join(literal(v) for v in values) + "]"


def archive_inputs(
    con: duckdb.DuckDBPyConnection, files: list[Path], grid_min: int = GRID_MIN
) -> None:
    """``ftarget`` and ``fdeparted`` from local archive months (``dbdelay archive-pull``).

    Training moments sit on a fixed clock grid from ``LOOKBACK_MIN`` before the planned
    arrival until the train actually arrives (or ``LOOKAHEAD_MIN`` after the planned time).
    They must not be picked from the outcome: with t = actual arrival - h for a few fixed h,
    the planned time left gives the delay away (h - min_to_planned) and the model learns
    that instead of the trains. "Not arrived yet" is the only outcome it uses, and that is
    known at t. ``horizon`` is then just the row key: minutes from t to the actual arrival.
    """
    src = "[" + ", ".join(literal(f.as_posix()) for f in files) + "]"
    step = f"interval {grid_min} minute"
    con.execute(
        f"""
        create or replace temp table a as
        select * from read_parquet({src})
        where not canceled and not is_diversion and not midnight_placeholder
          and coalesce(abs(arr_delay_min), 0) <= 720 and coalesce(abs(dep_delay_min), 0) <= 720;

        create or replace temp table ftarget as
        select stop_id, trip_key, eva, train_type, train_number, arr_planned, actual_delay,
               cast(date_diff('second', t, arr_changed) / 60 as integer) as horizon, t
        from (
            select *, arr_delay_min as actual_delay,
                   unnest(generate_series(
                       time_bucket({step}, arr_planned - interval {LOOKBACK_MIN} minute)
                           + {step},
                       least(arr_changed - interval 1 minute,
                             arr_planned + interval {LOOKAHEAD_MIN} minute),
                       {step})) as t
            from a
            where arr_planned is not null and arr_changed is not null
        );

        create or replace temp table fdeparted as
        select g.stop_id, g.horizon, k.eva as prev_eva, k.dep_planned as prev_planned,
               k.dep_changed as prev_dep
        from ftarget g
        join a k on k.trip_key = g.trip_key and k.stop_id <> g.stop_id
             and k.dep_planned < g.arr_planned
             and k.dep_changed is not null and k.dep_changed <= g.t;
        """
    )


def collector_inputs(con: duckdb.DuckDBPyConnection) -> None:
    """``ftarget`` and ``fdeparted`` from the temp tables of ``baselines.build``."""
    con.execute(
        """
        create or replace temp table ftarget as
        select n.stop_id, n.trip_key, n.eva, s.train_type, s.train_number, n.arr_planned,
               n.actual_delay, n.horizon, n.t
        from snapshots n join st s using (stop_id)
        where n.pers_delay is not null;

        create or replace temp table fdeparted as
        select stop_id, horizon, prev_eva, prev_planned, prev_dep from departed;
        """
    )


def build_features(
    con: duckdb.DuckDBPyConnection, stations: list[Station], history: list[Path]
) -> None:
    """Creates the temp table ``features``: ids, ``actual_delay`` and ``FEATURES``."""
    evas = _list([s.eva for s in stations])
    types = _list(LONG_DISTANCE)
    hist_src = "[" + ", ".join(literal(f.as_posix()) for f in history) + "]"
    con.execute(
        f"""
        create or replace temp table cur as
        with r as (
            select *, date_diff('second', prev_planned, prev_dep) / 60.0 as d,
                   row_number() over (partition by stop_id, horizon
                                      order by prev_planned desc) as rk,
                   count(*) over (partition by stop_id, horizon) as n
            from fdeparted
        )
        select stop_id, horizon,
               max(d) filter (where rk = 1) as cur_delay,
               max(d) filter (where rk = 2) as prev2_delay,
               max(prev_eva) filter (where rk = 1) as prev_eva,
               max(prev_planned) filter (where rk = 1) as prev_planned,
               max(prev_dep) filter (where rk = 1) as prev_dep,
               max(n) as n_departed
        from r where rk <= 2 group by stop_id, horizon;

        create or replace temp table hist as
        with keys as (
            select distinct train_number, eva, cast(arr_planned as date) as day from ftarget
        ),
        past as (
            select train_number, eva, cast(arr_planned as date) as day, arr_delay_min as d
            from read_parquet({hist_src})
            where arr_delay_min is not null and not canceled and not midnight_placeholder
              and abs(arr_delay_min) <= 720
        )
        select k.train_number, k.eva, k.day,
               median(p.d) as hist_median, quantile_cont(p.d, 0.9) as hist_p90,
               count(p.d) as hist_n
        from keys k
        join past p on p.train_number = k.train_number and p.eva = k.eva
             and p.day >= k.day - {HISTORY_DAYS} and p.day < k.day
        group by all;

        create or replace temp table features as
        select g.stop_id, g.trip_key, g.horizon, g.t, g.arr_planned, g.actual_delay,
               c.cur_delay,
               c.cur_delay - c.prev2_delay as delay_change,
               c.n_departed,
               date_diff('second', g.t, g.arr_planned) / 60.0 as min_to_planned,
               date_diff('second', g.t, g.arr_planned) / 60.0 + c.cur_delay as naive_eta,
               date_diff('second', c.prev_dep, g.t) / 60.0 as min_since_dep,
               date_diff('second', c.prev_planned, g.arr_planned) / 60.0 as planned_run,
               hour(g.t) as hour,
               dayofweek(g.t) as dow,
               list_position({evas}, g.eva) as eva_code,
               list_position({evas}, c.prev_eva) as prev_eva_code,
               list_position({types}, g.train_type) as type_code,
               h.hist_median, h.hist_p90, coalesce(h.hist_n, 0) as hist_n
        from ftarget g
        join cur c using (stop_id, horizon)
        left join hist h on h.train_number = g.train_number and h.eva = g.eva
             and h.day = cast(g.arr_planned as date);
        """
    )

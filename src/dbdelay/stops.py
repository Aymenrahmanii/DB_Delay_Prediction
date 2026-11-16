"""One stop table for both sources: the collector's Parquet tables and the archive.

Both queries return ``STOP_COLUMNS`` (the collector adds a few columns of its own), so a trip
can be read from either source with the same code, and week 3 can stack them.

For the collector, the *changed* time is DB's last published view of the stop. A stop that
appeared in a change feed without a changed time was on time (changed = planned); a stop that
never appeared in any fetch has ``observed = false`` and no changed time, because nothing
says whether the collector was running then.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from dbdelay.parse import STOP_ID_RE
from dbdelay.stations import Station
from dbdelay.tables import EVENTS_DIR, OBS_FILE, PLAN_FILE

# DB numbers the stops of a rerouted trip from 100 up; they can repeat a station and run out
# of order with the planned stops (seen in the collected data, November 2026).
DIVERSION_STOP_NUM = 100

# The trip start time inside a stop id <ride id>-<yymmddhhmm>-<stop number>.
TRIP_TIME_RE = r"-(\d{10})-\d+$"

# ECE (EuroCity-Express) is EuroCity class; DB lists it separately.
LONG_DISTANCE = ("ICE", "IC", "EC", "ECE")

STOP_COLUMNS = (
    "stop_id",
    "trip_key",
    "ride_id",
    "stop_num",
    "eva",
    "station_name",
    "train_type",
    "train_number",
    "line",
    "arr_planned",
    "arr_changed",
    "dep_planned",
    "dep_changed",
    "arr_delay_min",
    "dep_delay_min",
    "arr_canceled",
    "dep_canceled",
    "canceled",
    "is_added",
    "is_diversion",
    "platform_changed",
    "midnight_placeholder",
    "source",
)


def literal(value: str) -> str:
    """Quote a string for SQL. Only used for paths and ids we control."""
    return "'" + value.replace("'", "''") + "'"


def delay_columns_sql() -> str:
    """Delay and placeholder columns, computed the same way for both sources."""
    return f"""
        date_diff('second', arr_planned, arr_changed) / 60.0 as arr_delay_min,
        date_diff('second', dep_planned, dep_changed) / 60.0 as dep_delay_min,
        coalesce(arr_canceled, false) or coalesce(dep_canceled, false) as canceled,
        coalesce(stop_num >= {DIVERSION_STOP_NUM}, false) as is_diversion,
        (
            (arr_changed is not null and epoch(arr_changed) % 86400 = 0)
            or (dep_changed is not null and epoch(dep_changed) % 86400 = 0)
        ) as midnight_placeholder
    """


def _stations_sql(stations: list[Station]) -> str:
    rows = ", ".join(f"({literal(s.eva)}, {literal(s.name)})" for s in stations)
    return f"select * from (values {rows}) v(eva, station_name)"


def collector_stops_sql(
    tables_dir: Path, stations: list[Station], long_distance_only: bool = True
) -> str:
    d = tables_dir.as_posix()
    rx = literal(STOP_ID_RE.pattern)
    types = ", ".join(literal(t) for t in LONG_DISTANCE)
    where = f"where train_type in ({types})" if long_distance_only else ""
    return f"""
        with plan as (select * from read_parquet({literal(f"{d}/{PLAN_FILE}")})),
        ev as (select * from read_parquet({literal(f"{d}/{EVENTS_DIR}/*.parquet")})),
        obs as (select * from read_parquet({literal(f"{d}/{OBS_FILE}")})),
        last as (
            select * from ev
            qualify row_number() over (partition by stop_id order by observed_at desc) = 1
        ),
        n_ev as (select stop_id, count(*) as n_events from ev group by stop_id),
        added as (
            -- Long-distance stops that only the change feeds know: extra trains, and
            -- trains whose /plan hour was not fetched (collector down then).
            select
                stop_id,
                regexp_extract(stop_id, {rx}, 1) || '-' || regexp_extract(stop_id, {rx}, 2)
                    as trip_key,
                regexp_extract(stop_id, {rx}, 1) as ride_id,
                cast(nullif(regexp_extract(stop_id, {rx}, 3), '') as integer) as stop_num,
                any_value(eva) as eva,
                any_value(train_type) as train_type,
                any_value(train_number) as train_number,
                bool_or(trip_kind = 'e') as extra_train,
                cast(null as varchar) as line,
                any_value(arr_planned) as arr_planned,
                any_value(dep_planned) as dep_planned,
                cast(null as varchar) as arr_platform,
                cast(null as varchar) as dep_platform,
                false as in_plan
            from ev
            where train_type is not null
              and stop_id not in (select stop_id from plan)
            group by stop_id
        ),
        base as (
            select stop_id, trip_key, ride_id, stop_num, eva, train_type, train_number, line,
                   arr_planned, dep_planned, arr_platform, dep_platform,
                   false as extra_train, true as in_plan
            from plan
            union all by name
            select * from added
        ),
        joined as (
            select
                b.*,
                l.stop_id is not null as observed,
                case when l.stop_id is not null
                     then coalesce(l.arr_changed, b.arr_planned) end as arr_changed,
                case when l.stop_id is not null
                     then coalesce(l.dep_changed, b.dep_planned) end as dep_changed,
                l.arr_status = 'c' as arr_canceled,
                l.dep_status = 'c' as dep_canceled,
                b.extra_train or l.arr_status = 'a' or l.dep_status = 'a' as is_added,
                coalesce(l.arr_platform <> b.arr_platform, false)
                    or coalesce(l.dep_platform <> b.dep_platform, false) as platform_changed,
                coalesce(l.arr_platform, b.arr_platform) as arr_platform_final,
                coalesce(l.dep_platform, b.dep_platform) as dep_platform_final,
                -- Naive UTC: returning time-zone-aware values to Python needs pytz.
                timezone('UTC', o.first_seen) as first_seen,
                timezone('UTC', o.last_seen) as last_seen,
                o.n_fetches,
                coalesce(n.n_events, 0) as n_events
            from base b
            left join last l using (stop_id)
            left join obs o using (stop_id)
            left join n_ev n using (stop_id)
        )
        select
            j.stop_id, j.trip_key, j.ride_id, j.stop_num, j.eva, s.station_name,
            j.train_type, j.train_number, j.line,
            j.arr_planned, j.arr_changed, j.dep_planned, j.dep_changed,
            {delay_columns_sql()},
            coalesce(j.arr_canceled, false) as arr_canceled,
            coalesce(j.dep_canceled, false) as dep_canceled,
            coalesce(j.is_added, false) as is_added,
            j.platform_changed,
            'collector' as source,
            j.observed, j.in_plan, j.arr_platform, j.arr_platform_final, j.dep_platform,
            j.dep_platform_final, j.first_seen, j.last_seen, j.n_fetches, j.n_events
        from joined j
        left join ({_stations_sql(stations)}) s using (eva)
        {where}
    """


def archive_stops_sql(source: str, long_distance_only: bool = True) -> str:
    """The archive's ``monthly_processed_data`` mapped to ``STOP_COLUMNS``."""
    types = ", ".join(literal(t) for t in LONG_DISTANCE)
    where = f"where train_type in ({types})" if long_distance_only else ""
    return f"""
        select
            stop_id, trip_key, ride_id, stop_num, eva, station_name, train_type,
            train_number, line, arr_planned, arr_changed, dep_planned, dep_changed,
            {delay_columns_sql()},
            coalesce(arr_canceled, false) as arr_canceled,
            coalesce(dep_canceled, false) as dep_canceled,
            coalesce(is_added, false) as is_added,
            cast(null as boolean) as platform_changed,
            'archive' as source
        from (
            select
                id as stop_id,
                train_line_ride_id || '-'
                    || regexp_extract(id, {literal(TRIP_TIME_RE)}, 1) as trip_key,
                train_line_ride_id as ride_id,
                train_line_station_num as stop_num,
                -- The archive pads EVA numbers to 8 digits; the API uses 7.
                cast(try_cast(eva as bigint) as varchar) as eva,
                station_name,
                train_type,
                train_number,
                line_number as line,
                arrival_planned_time as arr_planned,
                arrival_change_time as arr_changed,
                departure_planned_time as dep_planned,
                departure_change_time as dep_changed,
                arrival_is_canceled as arr_canceled,
                departure_is_canceled as dep_canceled,
                is_additional_stop as is_added
            from read_parquet({literal(source)})
        )
        {where}
    """


def query(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def trip_stops(
    con: duckdb.DuckDBPyConnection, stops_sql: str, trip_key: str
) -> list[dict[str, Any]]:
    """All stops of one trip in planned order (a diversion's stop numbers do not follow it).
    A ride id alone is not a trip: it recurs on other days, so the key is the ride id plus
    the trip's start time from the stop id."""
    return query(
        con,
        f"""select * from ({stops_sql}) s where trip_key = {literal(trip_key)}
            order by coalesce(arr_planned, dep_planned), stop_num""",
    )


def find_trips(
    con: duckdb.DuckDBPyConnection, stops_sql: str, train: str, day: str
) -> list[dict[str, Any]]:
    """Trips of a train (``"ICE 597"`` or ``"597"``) with a stop on ``day`` (YYYY-MM-DD)."""
    kind, _, number = train.strip().rpartition(" ")
    type_filter = f"and train_type = {literal(kind.upper())}" if kind else ""
    return query(
        con,
        f"""
        select trip_key, train_type, train_number, count(*) as stops,
               min(coalesce(dep_planned, arr_planned)) as first_planned,
               max(coalesce(arr_planned, dep_planned)) as last_planned
        from ({stops_sql}) s
        where train_number = {literal(number)} {type_filter}
          and cast(coalesce(dep_planned, arr_planned) as date) = date {literal(day)}
        group by all
        order by first_planned
        """,
    )

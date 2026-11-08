"""Stop-level delays from the Hugging Face archive (piebro/deutsche-bahn-data).

The archive's ``monthly_processed_data`` table has one row per train, station and stop, with
planned and changed arrival and departure times (local Berlin time, naive). This module turns
it into a delay table: one row per stop with minutes late on arrival and departure. It never
downloads the file: DuckDB reads the Parquet over HTTP, with predicate pushdown.

Known data issue: some changed times are exactly midnight, which looks like a placeholder
rather than a real time. They are counted and flagged (``midnight_placeholder``), not dropped.
"""

from __future__ import annotations

from typing import Any

import duckdb

ARCHIVE_URL = "hf://datasets/piebro/deutsche-bahn-data/monthly_processed_data/data-{month}.parquet"
LONG_DISTANCE = ("ICE", "IC", "EC")
# Row ids look like <ride id>-<yymmddhhmm>-<stop number>; ride ids may be negative.
TRIP_TIME_RE = r"-(\d{10})-\d+$"


def archive_url(month: str) -> str:
    """``month`` is ``YYYY-MM``."""
    return ARCHIVE_URL.format(month=month)


def _literal(value: str) -> str:
    """Quote a string for SQL. Only used for paths and ids we control."""
    return "'" + value.replace("'", "''") + "'"


def stop_delays_sql(source: str, long_distance_only: bool = True) -> str:
    types = ", ".join(_literal(t) for t in LONG_DISTANCE)
    where = f"where train_type in ({types})" if long_distance_only else ""
    return f"""
        select
            id,
            train_line_ride_id as ride_id,
            train_line_ride_id || '-' || regexp_extract(id, {_literal(TRIP_TIME_RE)}, 1)
                as trip_key,
            train_line_station_num as stop_num,
            eva,
            station_name,
            train_type,
            train_number,
            arrival_planned_time as arr_planned,
            arrival_change_time as arr_changed,
            departure_planned_time as dep_planned,
            departure_change_time as dep_changed,
            date_diff('second', arrival_planned_time, arrival_change_time) / 60.0
                as arr_delay_min,
            date_diff('second', departure_planned_time, departure_change_time) / 60.0
                as dep_delay_min,
            (
                (arrival_change_time is not null
                 and epoch(arrival_change_time) % 86400 = 0)
                or (departure_change_time is not null
                    and epoch(departure_change_time) % 86400 = 0)
            ) as midnight_placeholder,
            arrival_is_canceled or departure_is_canceled as canceled
        from read_parquet({_literal(source)})
        {where}
    """


def trip_stops_sql(source: str, trip_key: str) -> str:
    return f"""
        select * from ({stop_delays_sql(source, long_distance_only=False)}) s
        where trip_key = {_literal(trip_key)}
        order by stop_num
    """


def connect(source: str = "") -> duckdb.DuckDBPyConnection:
    """A DuckDB connection; loads the HTTP extension only when reading a remote source."""
    con = duckdb.connect()
    if source.startswith(("hf://", "http://", "https://")):
        con.execute("install httpfs; load httpfs;")
    return con


def trip_stops(con: duckdb.DuckDBPyConnection, source: str, trip_key: str) -> list[dict[str, Any]]:
    """Stops of one trip. A ride id alone is not a trip: the same ride id recurs on other
    days, so the trip key is the ride id plus the 10-digit timestamp in the row id."""
    cur = con.execute(trip_stops_sql(source, trip_key))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def quality_report(con: duckdb.DuckDBPyConnection, source: str) -> dict[str, float]:
    """Counts for the week-2 checks: missing times, midnight placeholders, cancellations."""
    row = con.execute(
        f"""
        select
            count(*) as stops,
            count(*) filter (where arr_planned is null and dep_planned is null) as no_plan,
            count(*) filter (where arr_changed is null and dep_changed is null) as no_change,
            count(*) filter (where midnight_placeholder) as midnight_placeholder,
            count(*) filter (where canceled) as canceled,
            count(*) filter (where trip_key is null) as bad_key,
            count(*) filter (where abs(arr_delay_min) > 720 or abs(dep_delay_min) > 720)
                as implausible_over_12h
        from ({stop_delays_sql(source)})
        """
    ).fetchone()
    assert row is not None
    keys = [
        "stops",
        "no_plan",
        "no_change",
        "midnight_placeholder",
        "canceled",
        "bad_key",
        "implausible_over_12h",
    ]
    return {k: float(v) for k, v in zip(keys, row, strict=True)}

"""Stop-level delays from the Hugging Face archive (piebro/deutsche-bahn-data).

The archive's ``monthly_processed_data`` table has one row per train, station and stop, with
planned and changed arrival and departure times (local Berlin time, naive). This module turns
it into a delay table: one row per stop with minutes late on arrival and departure. It never
downloads the file: DuckDB reads the Parquet over HTTP, with predicate pushdown.

Known data issue: some changed times are exactly midnight, which looks like a placeholder
rather than a real time. They are counted and flagged (``midnight_placeholder``), not dropped.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from dbdelay import stops
from dbdelay.stops import archive_stops_sql

ARCHIVE_URL = "hf://datasets/piebro/deutsche-bahn-data/monthly_processed_data/data-{month}.parquet"


def archive_url(month: str) -> str:
    """``month`` is ``YYYY-MM``."""
    return ARCHIVE_URL.format(month=month)


def stop_delays_sql(source: str, long_distance_only: bool = True) -> str:
    """The archive as the shared stop table (see ``dbdelay.stops``)."""
    return archive_stops_sql(source, long_distance_only)


def connect(source: str = "") -> duckdb.DuckDBPyConnection:
    """A DuckDB connection; loads the HTTP extension only when reading a remote source."""
    con = duckdb.connect()
    if source.startswith(("hf://", "http://", "https://")):
        con.execute("install httpfs; load httpfs;")
    return con


def trip_stops(con: duckdb.DuckDBPyConnection, source: str, trip_key: str) -> list[dict[str, Any]]:
    """Stops of one trip from the archive, in order."""
    return stops.trip_stops(con, stop_delays_sql(source, long_distance_only=False), trip_key)


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


def pull_month(month: str, evas: list[str], out_dir: Path) -> tuple[Path, int]:
    """Save one month of long-distance stops at the given stations as a local Parquet file,
    in the shared stop schema, so training does not re-read the remote archive."""
    src = archive_url(month)
    con = connect(src)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"stops-{month}.parquet"
    in_list = ", ".join(stops.literal(e) for e in evas)
    con.execute(
        f"""copy (select * from ({stop_delays_sql(src)}) s where eva in ({in_list}))
            to {stops.literal(out.as_posix())} (format parquet)"""
    )
    row = con.execute(f"select count(*) from {stops.literal(out.as_posix())}").fetchone()
    return out, int(row[0]) if row else 0


CHANGES_URL = (
    "hf://datasets/piebro/deutsche-bahn-data/monthly_processed_data_change/data-{month}.parquet"
)


def pull_changes(month: str, evas: list[str], out_dir: Path) -> tuple[Path, int]:
    """Save one month of the archive's change snapshots at the given stations.

    Each snapshot is DB's change feed for one station at one moment, so it holds DB's
    forecast as of then. ``snapshot_timestamp`` is UTC (naive): checked against the final
    times, the snapshot forecasts stop changing exactly 2 h "before" arrival in summer time.
    It is stored as Berlin local time (``snap_at``), like every other time here.
    """
    src = CHANGES_URL.format(month=month)
    con = connect(src)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"changes-{month}.parquet"
    in_list = ", ".join(stops.literal(e) for e in evas)
    con.execute(
        f"""copy (
            select id as stop_id, eva,
                   timezone('Europe/Berlin', timezone('UTC', snapshot_timestamp)) as snap_at,
                   arrival_change_time as arr_changed, arrival_change_status as arr_status
            from (select * replace (cast(try_cast(eva as bigint) as varchar) as eva)
                  from read_parquet({stops.literal(src)}))
            where eva in ({in_list})
        ) to {stops.literal(out.as_posix())} (format parquet)"""
    )
    row = con.execute(f"select count(*) from {stops.literal(out.as_posix())}").fetchone()
    return out, int(row[0]) if row else 0

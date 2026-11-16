from pathlib import Path

import duckdb
import pytest

from dbdelay.archive import (
    archive_url,
    connect,
    quality_report,
    stop_delays_sql,
    trip_stops,
)


@pytest.fixture
def sample(tmp_path: Path) -> str:
    """A tiny stand-in for one month of the archive, written as Parquet."""
    path = tmp_path / "sample.parquet"
    con = duckdb.connect()
    con.execute(
        """
        create table t as select * from (values
          ('r1-2610011000-1', 'r1', 1, '08000105', 'Frankfurt (Main) Hbf', 'ICE', '1021',
           TIMESTAMP '2026-10-01 10:00', TIMESTAMP '2026-10-01 10:04',
           TIMESTAMP '2026-10-01 10:02', TIMESTAMP '2026-10-01 10:09', false, false),
          ('r1-2610011000-2', 'r1', 2, '08000244', 'Mannheim Hbf', 'ICE', '1021',
           TIMESTAMP '2026-10-01 10:50', TIMESTAMP '2026-10-01 11:00',
           TIMESTAMP '2026-10-01 10:51', TIMESTAMP '2026-10-01 11:01', false, false),
          ('r2-2610011000-1', 'r2', 1, '08000207', 'Köln Hbf', 'RE', '4711',
           TIMESTAMP '2026-10-01 10:00', TIMESTAMP '2026-10-01 10:00',
           TIMESTAMP '2026-10-01 10:00', TIMESTAMP '2026-10-01 10:00', false, false),
          ('r3-2610011200-1', 'r3', 1, '08000152', 'Hannover Hbf', 'IC', '2021',
           TIMESTAMP '2026-10-01 12:00', TIMESTAMP '2026-10-02 00:00',
           NULL, NULL, true, false),
          ('r4-2610011200-1', 'r4', 1, '08000080', 'Dortmund Hbf', 'EC', '55',
           TIMESTAMP '2026-10-01 12:00', TIMESTAMP '2026-10-01 14:30',
           TIMESTAMP '2026-10-01 12:00', TIMESTAMP '2026-10-01 14:31', false, false)
        ) v(id, ride, stop, eva, station_name, train_type, train_number,
            arrival_planned_time, arrival_change_time,
            departure_planned_time, departure_change_time,
            arrival_is_canceled, departure_is_canceled)
        """
    )
    con.execute(
        f"""copy (select id, ride as train_line_ride_id, stop as train_line_station_num,
            eva, station_name, train_type, train_number, arrival_planned_time,
            arrival_change_time, departure_planned_time, departure_change_time,
            arrival_is_canceled, departure_is_canceled,
            'L' || ride as line_number, false as is_additional_stop from t)
            to '{path.as_posix()}' (format parquet)"""
    )
    return path.as_posix()


def test_archive_url_is_monthly_processed_table() -> None:
    assert archive_url("2026-10").endswith("monthly_processed_data/data-2026-10.parquet")


def test_trip_stops_orders_and_computes_delays(sample: str) -> None:
    con = connect(sample)
    stops = trip_stops(con, sample, "r1-2610011000")
    assert [s["stop_num"] for s in stops] == [1, 2]
    assert stops[0]["arr_delay_min"] == pytest.approx(4.0)
    assert stops[0]["dep_delay_min"] == pytest.approx(7.0)
    assert stops[1]["dep_delay_min"] == pytest.approx(10.0)
    assert str(stops[0]["arr_planned"]) == "2026-10-01 10:00:00"


def test_long_distance_filter_drops_regional(sample: str) -> None:
    con = connect(sample)
    rows = con.execute(stop_delays_sql(sample)).fetchall()
    assert len(rows) == 5 - 1  # the RE train is excluded
    all_rows = con.execute(stop_delays_sql(sample, long_distance_only=False)).fetchall()
    assert len(all_rows) == 5


def test_midnight_placeholder_is_flagged_not_dropped(sample: str) -> None:
    con = connect(sample)
    stops = trip_stops(con, sample, "r3-2610011200")
    assert stops[0]["midnight_placeholder"] is True
    assert stops[0]["canceled"] is True


def test_quality_report_counts(sample: str) -> None:
    report = quality_report(connect(sample), sample)
    assert report["stops"] == 4
    assert report["midnight_placeholder"] == 1
    assert report["canceled"] == 1
    assert report["bad_key"] == 0
    assert report["no_change"] == 0
    assert report["implausible_over_12h"] == 0

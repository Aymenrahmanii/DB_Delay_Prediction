"""Feature rows from a tiny archive, with the leakage rules checked explicitly."""

from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dbdelay.features import FEATURES, archive_inputs, build_features
from dbdelay.stations import load_stations
from dbdelay.stops import query

FFM, MA = "8000105", "8000244"
TARGET = "1-2610100900-2"


def _archive(path: Path, arrival: str) -> Path:
    """ICE 597 leaves Frankfurt 10:05 (+5) and reaches Mannheim at ``arrival`` (plan 10:40),
    plus the same train on two other days for the history features."""
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.connect().execute(
        f"""
        copy (
          select *, date_diff('second', arr_planned, arr_changed) / 60.0 as arr_delay_min,
                 date_diff('second', dep_planned, dep_changed) / 60.0 as dep_delay_min,
                 false as canceled, false as is_diversion, false as midnight_placeholder
          from (values
            ('1-2610100900-1', '1-2610100900', '{FFM}', 'ICE', '597',
             null, null, timestamp '2026-10-10 10:00', timestamp '2026-10-10 10:05'),
            ('{TARGET}', '1-2610100900', '{MA}', 'ICE', '597',
             timestamp '2026-10-10 10:40', timestamp '2026-10-10 {arrival}', null, null),
            ('2-2610090900-2', '2-2610090900', '{MA}', 'ICE', '597',
             timestamp '2026-10-09 10:40', timestamp '2026-10-09 10:44', null, null),
            ('3-2610110900-2', '3-2610110900', '{MA}', 'ICE', '597',
             timestamp '2026-10-11 10:40', timestamp '2026-10-11 12:19', null, null)
          ) v(stop_id, trip_key, eva, train_type, train_number,
              arr_planned, arr_changed, dep_planned, dep_changed)
        ) to '{path.as_posix()}' (format parquet)
        """
    )
    return path


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    return _archive(tmp_path / "stops-2026-10.parquet", "10:50")


def _features(archive: Path) -> dict[datetime, dict[str, Any]]:
    con = duckdb.connect()
    archive_inputs(con, [archive])
    build_features(con, load_stations(), [archive])
    return {r["t"]: r for r in query(con, f"select * from features where stop_id = '{TARGET}'")}


def at(hh: int, mm: int) -> datetime:
    return datetime(2026, 10, 10, hh, mm)  # noqa: DTZ001 - naive Berlin


def test_horizon_is_not_a_feature() -> None:
    assert "horizon" not in FEATURES and "t" not in FEATURES


def test_rows_on_the_grid_while_running(archive: Path) -> None:
    # Every 15 min, after the train left Frankfurt (10:05) and before it arrived (10:50).
    assert sorted(_features(archive)) == [at(10, 15), at(10, 30), at(10, 45)]


def test_feature_values(archive: Path) -> None:
    f = _features(archive)[at(10, 30)]
    assert f["actual_delay"] == 10 and f["horizon"] == 20
    assert f["cur_delay"] == 5 and f["n_departed"] == 1
    assert f["min_to_planned"] == 10 and f["naive_eta"] == 15
    assert f["min_since_dep"] == 25 and f["planned_run"] == 40


def test_moments_do_not_depend_on_the_outcome(tmp_path: Path) -> None:
    # The same train arriving 13 min later: the moments before 10:50 are the same, and their
    # features are identical; only the label and the "not arrived yet" moments differ.
    early = _features(_archive(tmp_path / "a" / "stops-2026-10.parquet", "10:50"))
    late = _features(_archive(tmp_path / "b" / "stops-2026-10.parquet", "11:03"))
    assert set(early) <= set(late)
    for t in early:
        assert [early[t][f] for f in FEATURES] == [late[t][f] for f in FEATURES]
    assert set(late) - set(early) == {at(11, 0)}


def test_history_uses_earlier_days_only(archive: Path) -> None:
    f = _features(archive)[at(10, 30)]
    # Only 9 October counts: not the target day itself, not 11 October (+99).
    assert (f["hist_median"], f["hist_n"]) == (4, 1)

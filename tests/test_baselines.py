"""Baselines on one hand-made trip with known answers (Berlin = UTC+1 in November)."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from dbdelay import baselines as bl
from dbdelay.stations import load_stations
from dbdelay.storage import PollLog, PollResult, RawRecord, append_raw, raw_file
from dbdelay.tables import build_tables

pytest.importorskip("pyarrow")

FFM, MA = "8000105", "8000244"
TRIP = "123-2611131100"
PLAN_FFM = f"""<timetable><s id="{TRIP}-3"><tl f="F" t="p" c="ICE" n="597"/>
  <ar pt="2611131158"/><dp pt="2611131200"/></s></timetable>""".encode()
PLAN_MA = f"""<timetable><s id="{TRIP}-5"><tl f="F" t="p" c="ICE" n="597"/>
  <ar pt="2611131240"/><dp pt="2611131242"/></s></timetable>""".encode()


def berlin(hh: int, mm: int) -> datetime:
    return datetime(2026, 11, 13, hh - 1, mm, tzinfo=UTC)


def change(eva: str, num: int, kind: str, ct: str) -> bytes:
    return f"""<timetable eva="{eva}"><s id="{TRIP}-{num}" eva="{eva}">
      <{kind} ct="2611131{ct}"/></s></timetable>""".encode()


def write(data: Path, endpoint: str, eva: str, at: datetime, body: bytes) -> None:
    rec = RawRecord(endpoint, eva, "/x", at.isoformat(), 200, body)  # type: ignore[arg-type]
    append_raw(raw_file(data / "raw", endpoint, eva, at), rec)  # type: ignore[arg-type]


@pytest.fixture
def data(tmp_path: Path) -> Path:
    write(tmp_path, "plan", FFM, berlin(11, 0), PLAN_FFM)
    write(tmp_path, "plan", MA, berlin(11, 0), PLAN_MA)
    write(tmp_path, "fchg", FFM, berlin(11, 0), change(FFM, 3, "dp", "205"))  # leaves +5
    write(tmp_path, "fchg", MA, berlin(11, 0), change(MA, 5, "ar", "243"))  # DB: +3
    write(tmp_path, "fchg", MA, berlin(12, 10), change(MA, 5, "ar", "248"))  # DB: +8
    write(tmp_path, "fchg", MA, berlin(12, 50), change(MA, 5, "ar", "250"))  # last word: +10
    write(tmp_path, "fchg", MA, berlin(13, 30), change(MA, 5, "ar", "253"))  # moved after
    log = PollLog(tmp_path / "polls")
    t = berlin(10, 0)
    while t <= berlin(13, 30):
        for eva in (FFM, MA):
            iso = t.isoformat()
            log.write(PollResult("fchg", eva, iso, iso, True, 200, 1, 100, 0.1), t)
        t += timedelta(minutes=5)
    build_tables(tmp_path / "raw", tmp_path / "tables")
    return tmp_path


@pytest.fixture
def con(data: Path) -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    bl.build(c, bl.Paths(data / "tables", data / "polls"), load_stations())
    return c


def test_actual_is_the_last_word_before_arrival(con: duckdb.DuckDBPyConnection) -> None:
    row = con.execute(
        f"select arr_actual, covered, moved_after from act where stop_id = '{TRIP}-5'"
    )
    assert row.fetchone() == (datetime(2026, 11, 13, 12, 50), True, True)  # noqa: DTZ001


def test_snapshots_use_only_what_was_known(con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute(
        f"""select horizon, t, actual_delay, db_delay, pers_delay from snapshots
            where stop_id = '{TRIP}-5' order by horizon"""
    ).fetchall()
    by_h = {r[0]: r[1:] for r in rows}
    # 15 min before the actual 12:50: DB said 12:48; the train had left Frankfurt at +5.
    assert by_h[15] == (datetime(2026, 11, 13, 12, 35), 10.0, 8.0, 5.0)  # noqa: DTZ001
    # 60 min before: DB said 12:43, and the train had not left Frankfurt yet.
    assert by_h[60] == (datetime(2026, 11, 13, 11, 50), 10.0, 3.0, None)  # noqa: DTZ001


def test_scores_only_running_trains(con: duckdb.DuckDBPyConnection) -> None:
    scores = {r["horizon"]: r for r in bl.score(con)}
    assert set(scores) == {15, 30}
    s = scores[15]
    assert (s["n"], s["mae_timetable"], s["mae_persistence"], s["mae_db"]) == (1, 10, 5, 2)


def test_no_poll_no_snapshot(tmp_path: Path, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "delete from polls where polled_at between timestamp '2026-11-13 12:20' and "
        "timestamp '2026-11-13 12:40'"
    )
    con.execute("drop table snapshots")
    bl._snapshots(con, bl.HORIZONS_MIN, "actual")
    hs = con.execute(
        f"select horizon from snapshots where stop_id = '{TRIP}-5' order by 1"
    ).fetchall()
    assert [h for (h,) in hs] == [30, 60, 90]  # no poll of Mannheim in the 10 min before 12:35


def test_score_a_day_of_forecasts(data: Path) -> None:
    import json

    from dbdelay.score import score_day, totals

    def forecast(made: str, pred: float, db: float, cur: float) -> str:
        return json.dumps(
            {
                "made_at": f"2026-11-13T{made}:00",
                "stop_id": f"{TRIP}-5",
                "trip_key": TRIP,
                "eva": MA,
                "model": "with_db",
                "pred_delay": pred,
                "lo": pred - 4,
                "hi": pred + 3,
                "db_delay": db,
                "cur_delay": cur,
                "arr_planned": "2026-11-13T12:40",
                "run_id": "r1",
            }
        )

    log = data / "predictions" / "2026-11-13.jsonl"
    log.parent.mkdir()
    # The train arrived 12:50 (+10). Made 12:35 is 15 min before; 12:33 is a worse match.
    lines = [
        forecast("12:33", 2, 8, 5),
        forecast("12:35", 9, 8, 5),
        forecast("11:50", 4, 3, 0),
        forecast("12:20", 7, 8, 5),  # 30 min before
    ]
    log.write_text("".join(line + "\n" for line in lines))
    score = score_day(
        bl.Paths(data / "tables", data / "polls"),
        load_stations(),
        log,
        date(2026, 11, 13),
        "replay",
    )
    rows = {r["horizon"]: r for r in score.rows}
    assert score.forecasts == 4 and score.run_ids == ["r1"]
    assert set(rows) == {15, 30, 60}
    r15 = rows[15]
    assert (r15["n"], r15["mae_model"], r15["mae_db"], r15["mae_persistence"]) == (1, 1, 2, 5)
    assert r15["coverage"] == 1 and r15["precision_model"] == 1 and r15["recall_db"] == 1
    assert rows[60]["mae_model"] == 6
    total = {r["horizon"]: r for r in totals([score], "with_db")}
    assert total[15]["gain_vs_db"] == pytest.approx(0.5)

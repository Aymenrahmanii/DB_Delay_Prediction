from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from dbdelay.checks import check_day
from dbdelay.config import Settings
from dbdelay.parse import db_time, parse_changes, parse_plan, split_stop_id
from dbdelay.stations import load_stations
from dbdelay.stops import collector_stops_sql, find_trips, trip_stops
from dbdelay.storage import RawRecord, append_raw, raw_file
from dbdelay.tables import build_tables

pytest.importorskip("pyarrow")

FFM, MA = "8000105", "8000244"
ICE = "123-2611131000"  # ICE 597, stops 3 (Frankfurt) and 5 (Mannheim)
IC = "-456-2611131100"  # IC 2021, negative ride id, cancelled at Frankfurt
EXTRA = "789-2611131130"  # an extra ICE that is not in the plan
UNPLANNED = "555-2611130900"  # a planned ICE whose /plan hour was never fetched

PLAN_FFM = f"""<timetable station="Frankfurt(Main)Hbf">
<s id="{ICE}-3"><tl f="F" t="p" o="80" c="ICE" n="597"/>
  <ar pt="2611131200" pp="7" l="" ppth="Köln Hbf|Frankfurt(M) Flughafen Fernbf"/>
  <dp pt="2611131205" pp="7" ppth="Mannheim Hbf|Stuttgart Hbf"/></s>
<s id="{IC}-2"><tl f="F" t="p" o="80" c="IC" n="2021"/><dp pt="2611131230" pp="4"/></s>
<s id="999-2611131100-4"><tl f="N" t="p" o="800" c="RE" n="4711"/><dp pt="2611131210"/></s>
</timetable>""".encode()
PLAN_MA = f"""<timetable station="Mannheim Hbf">
<s id="{ICE}-5"><tl f="F" t="p" o="80" c="ICE" n="597"/>
  <ar pt="2611131240" pp="3"/><dp pt="2611131242" pp="3"/></s>
<s id="{ICE}-100"><tl f="F" t="p" o="80" c="ICE" n="597"/>
  <ar pt="2611131225" pp="1"/><dp pt="2611131226" pp="1"/></s>
</timetable>""".encode()
FCHG_FFM_1 = f"""<timetable station="Frankfurt(Main)Hbf" eva="{FFM}">
<s id="{ICE}-3" eva="{FFM}"><ar ct="2611131204"/><dp ct="2611131208"/></s>
<s id="{IC}-2" eva="{FFM}"><dp cs="c" clt="2611131101"/></s>
<s id="999-2611131100-4" eva="{FFM}"><dp ct="2611131215"/></s>
<s id="{EXTRA}-1" eva="{FFM}"><tl f="F" t="e" c="ICE" n="9999"/>
  <dp pt="2611131130" ct="2611131135"/></s>
<s id="{UNPLANNED}-2" eva="{FFM}"><tl f="F" t="p" c="ICE" n="500"/>
  <ar pt="2611131050" ct="2611131052"/></s>
</timetable>""".encode()
RCHG_FFM = f"""<timetable station="Frankfurt(Main)Hbf" eva="{FFM}">
<s id="{ICE}-3" eva="{FFM}"><ar ct="2611131206" cp="9"/><dp ct="2611131210" cp="9"/></s>
</timetable>""".encode()
FCHG_MA = f"""<timetable station="Mannheim Hbf" eva="{MA}">
<s id="{ICE}-5" eva="{MA}"><ar ct="2611131245"/><dp ct="2611131247"/></s>
</timetable>""".encode()


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        db_client_id="id",  # type: ignore[arg-type]
        db_api_key="key",  # type: ignore[arg-type]
        data_dir=tmp_path,
        _env_file=None,  # type: ignore[call-arg]
    )


def _write(settings: Settings, endpoint: str, eva: str, at: datetime, body: bytes) -> None:
    rec = RawRecord(endpoint, eva, f"/{endpoint}/{eva}", at.isoformat(), 200, body)  # type: ignore[arg-type]
    append_raw(raw_file(settings.raw_dir, endpoint, eva, at), rec)  # type: ignore[arg-type]


def at(hh: int, mm: int) -> datetime:
    return datetime(2026, 11, 13, hh, mm, tzinfo=UTC)


@pytest.fixture
def built(tmp_path: Path) -> Settings:
    s = _settings(tmp_path)
    _write(s, "plan", FFM, at(10, 0), PLAN_FFM)
    _write(s, "plan", FFM, at(10, 30), PLAN_FFM)  # re-fetch of the same slot
    _write(s, "plan", MA, at(10, 0), PLAN_MA)
    _write(s, "fchg", FFM, at(10, 0), FCHG_FFM_1)
    _write(s, "fchg", FFM, at(10, 0), FCHG_FFM_1)  # a second collector, same slot
    _write(s, "fchg", FFM, at(10, 5), FCHG_FFM_1)  # nothing changed
    _write(s, "rchg", FFM, at(10, 6), RCHG_FFM)
    # Two collectors fetch Mannheim at the same instant and DB answers differently.
    _write(s, "rchg", MA, at(10, 7), FCHG_MA.replace(b"2611131245", b"2611131244"))
    _write(s, "rchg", MA, at(10, 7), FCHG_MA.replace(b"2611131245", b"2611131246"))
    _write(s, "fchg", MA, at(10, 10), FCHG_MA)
    build_tables(s.raw_dir, tmp_path / "tables")
    return s


def _stops(settings: Settings) -> str:
    return collector_stops_sql(settings.data_dir / "tables", load_stations())


def test_split_stop_id_handles_negative_ride_ids() -> None:
    sid = split_stop_id("-456-2611131100-2")
    assert sid is not None
    assert (sid.ride_id, sid.trip_key, sid.stop_num) == ("-456", "-456-2611131100", 2)
    assert split_stop_id("garbage") is None
    assert db_time("2611131205") == datetime(2026, 11, 13, 12, 5)  # noqa: DTZ001 - naive Berlin


def test_parse_plan_and_changes() -> None:
    plan = parse_plan(PLAN_FFM, FFM)
    assert [p.train_type for p in plan] == ["ICE", "IC", "RE"]
    assert plan[0].origin == "Köln Hbf" and plan[0].destination == "Stuttgart Hbf"
    assert [p.is_long_distance for p in plan] == [True, True, False]
    changes = {c.stop_id: c for c in parse_changes(FCHG_FFM_1, FFM)}
    assert changes[f"{IC}-2"].dep_status == "c"
    assert changes[f"{EXTRA}-1"].train_number == "9999"


def test_trip_joins_across_stations(built: Settings) -> None:
    rows = trip_stops(duckdb.connect(), _stops(built), ICE)
    # A diversion stop (number 100+) sorts by its planned time, not its number.
    assert [(r["stop_num"], r["station_name"], r["is_diversion"]) for r in rows] == [
        (3, "Frankfurt(Main)Hbf", False),
        (100, "Mannheim Hbf", True),
        (5, "Mannheim Hbf", False),
    ]
    ffm, _, ma = rows
    # The rchg fetch at 10:06 is the latest view of Frankfurt.
    assert ffm["arr_delay_min"] == pytest.approx(6) and ffm["dep_delay_min"] == pytest.approx(5)
    assert ffm["platform_changed"] is True and ffm["n_events"] == 2
    assert ma["arr_delay_min"] == pytest.approx(5) and ma["platform_changed"] is False


def test_repeats_and_duplicate_collectors_are_one_event(built: Settings) -> None:
    con = duckdb.connect()
    ev = (built.data_dir / "tables" / "change_events" / "*.parquet").as_posix()
    n = con.execute(f"select count(*) from '{ev}' where stop_id = '{ICE}-3'").fetchone()
    assert n == (2,)
    obs_file = (built.data_dir / "tables" / "stop_observations.parquet").as_posix()
    obs = con.execute(f"select n_fetches from '{obs_file}' where stop_id = '{ICE}-3'").fetchone()
    assert obs == (4,)


def test_cancelled_added_and_regional(built: Settings) -> None:
    con = duckdb.connect()
    ic = trip_stops(con, _stops(built), IC)
    assert ic[0]["canceled"] is True and ic[0]["dep_delay_min"] == pytest.approx(0)
    extra = trip_stops(con, _stops(built), EXTRA)
    assert extra[0]["is_added"] is True and extra[0]["dep_delay_min"] == pytest.approx(5)
    assert extra[0]["in_plan"] is False
    unplanned = trip_stops(con, _stops(built), UNPLANNED)
    assert unplanned[0]["is_added"] is False and unplanned[0]["in_plan"] is False
    assert unplanned[0]["arr_delay_min"] == pytest.approx(2)
    assert trip_stops(con, _stops(built), "999-2611131100") == []  # RE is not kept


def test_find_trips_by_train_and_day(built: Settings) -> None:
    con = duckdb.connect()
    trips = find_trips(con, _stops(built), "ICE 597", "2026-11-13")
    assert [t["trip_key"] for t in trips] == [ICE]
    assert find_trips(con, _stops(built), "ICE 597", "2026-11-14") == []


def test_checks_pass_on_clean_tables(built: Settings) -> None:
    results = check_day(
        duckdb.connect(), built, load_stations(), built.data_dir / "tables", date(2026, 11, 13)
    )
    errors = [r.format() for r in results if r.level == "error" and not r.ok]
    assert errors == []
    by_name = {r.name: r for r in results}
    assert by_name["duplicate change events"].bad == 0
    assert not by_name["hours with under 95 % of fchg polls"].ok  # no poll log in this test


def test_same_instant_fetches_give_one_event(built: Settings) -> None:
    con = duckdb.connect()
    ev = (built.data_dir / "tables" / "change_events" / "*.parquet").as_posix()
    rows = con.execute(
        f"select arr_changed from '{ev}' where stop_id = '{ICE}-5' order by observed_at"
    ).fetchall()
    assert rows == [  # the first of the two 10:07 fetches, then the 10:10 one
        (datetime(2026, 11, 13, 12, 44),),  # noqa: DTZ001 - naive Berlin
        (datetime(2026, 11, 13, 12, 45),),  # noqa: DTZ001
    ]

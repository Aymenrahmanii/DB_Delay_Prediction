"""Live state, predictions and the web app on one hand-made trip (Berlin = UTC+1)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from dbdelay.features import FEATURES, FEATURES_DB
from dbdelay.live import LiveState, predict
from dbdelay.model import QuantileModel
from dbdelay.stations import load_stations
from dbdelay.storage import RawRecord, append_raw, raw_file

pytest.importorskip("fastapi")

FFM, MA = "8000105", "8000244"
TRIP = "123-2611131000"
NOW = datetime(2026, 11, 13, 10, 30, tzinfo=UTC)  # 11:30 in Berlin
PLAN_FFM = f"""<timetable><s id="{TRIP}-3"><tl f="F" t="p" c="ICE" n="597"/>
  <dp pt="2611131120" pp="7"/></s></timetable>""".encode()
PLAN_MA = f"""<timetable><s id="{TRIP}-5"><tl f="F" t="p" c="ICE" n="597"/>
  <ar pt="2611131200" pp="3"/><dp pt="2611131202" pp="3"/></s></timetable>""".encode()


def fchg(eva: str, num: int, kind: str, ct: str) -> bytes:
    return f"""<timetable eva="{eva}"><s id="{TRIP}-{num}" eva="{eva}">
      <{kind} ct="261113{ct}"/></s></timetable>""".encode()


def write(
    raw: Path, endpoint: str, eva: str, at: datetime, body: bytes, partition: datetime | None = None
) -> None:
    rec = RawRecord(endpoint, eva, "/x", at.isoformat(), 200, body)  # type: ignore[arg-type]
    append_raw(raw_file(raw, endpoint, eva, partition or at), rec)  # type: ignore[arg-type]


class FakeBooster:
    best_iteration = 0

    def __init__(self, value: float) -> None:
        self.value = value

    def predict(self, x: Any, num_iteration: int = 0) -> Any:
        return np.full(len(x), self.value)


def fake(name: str, features: tuple[str, ...], q10: float, q50: float, q90: float) -> QuantileModel:
    boosters = {0.1: FakeBooster(q10), 0.5: FakeBooster(q50), 0.9: FakeBooster(q90)}
    return QuantileModel(name, features, boosters, 1.0, {})


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    # plan files are partitioned by the requested slot, in Berlin time
    slot = datetime(2026, 11, 13, 11, 0)  # noqa: DTZ001
    write(raw, "plan", FFM, NOW - timedelta(hours=1), PLAN_FFM, slot)
    write(raw, "plan", MA, NOW - timedelta(hours=1), PLAN_MA, slot.replace(hour=12))
    write(raw, "fchg", FFM, NOW - timedelta(minutes=5), fchg(FFM, 3, "dp", "1124"))  # left +4
    write(raw, "fchg", MA, NOW - timedelta(minutes=5), fchg(MA, 5, "ar", "1206"))  # DB: +6
    return raw


def test_predicts_the_coming_stop(raw: Path) -> None:
    state = LiveState(raw, load_stations())
    assert state.refresh(NOW) == 2
    models = [fake("with_db", FEATURES_DB, 3, 7, 10), fake("without_db", FEATURES, 1, 5, 9)]
    preds = {p.model: p for p in predict(state, models, load_stations(), [], NOW)}
    p = preds["with_db"]
    assert p.stop_id == f"{TRIP}-5" and p.made_at == "2026-11-13T11:30:00"
    assert (p.cur_delay, p.db_delay) == (4, 6)
    assert (p.pred_delay, p.lo, p.hi) == (7, 2, 11)  # range widened by 1 min each side
    assert preds["without_db"].pred_delay == 5


def test_refresh_reads_only_new_records(raw: Path) -> None:
    state = LiveState(raw, load_stations())
    state.refresh(NOW)
    assert state.refresh(NOW) == 0
    write(raw, "fchg", MA, NOW, fchg(MA, 5, "ar", "1210"))
    assert state.refresh(NOW + timedelta(seconds=30)) == 1
    view = state.stops[f"{TRIP}-5"]
    assert view.arr == datetime(2026, 11, 13, 12, 10)  # noqa: DTZ001 - naive Berlin
    assert [a for _, a in view.track] == [
        datetime(2026, 11, 13, 12, 6),  # noqa: DTZ001
        datetime(2026, 11, 13, 12, 10),  # noqa: DTZ001
    ]


def test_no_forecast_without_recent_polls(raw: Path) -> None:
    state = LiveState(raw, load_stations())
    state.refresh(NOW)
    later = NOW + timedelta(minutes=20)  # nothing fetched for 25 minutes
    assert (
        predict(state, [fake("with_db", FEATURES_DB, 3, 7, 10)], load_stations(), [], later) == []
    )


def test_site(raw: Path, tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    from dbdelay.web.app import Live, create_app

    live = Live(
        state=LiveState(raw, load_stations()),
        models=[fake("with_db", FEATURES_DB, 3, 7, 10)],
        run_id="test",
        stations=load_stations(),
        history=[],
        predictions_dir=tmp_path / "predictions",
    )
    assert live.cycle(NOW) == 1
    logged = (tmp_path / "predictions" / "2026-11-13.jsonl").read_text().splitlines()
    assert json.loads(logged[0])["pred_delay"] == 7

    client = TestClient(create_app(live, run_loop=False))
    assert "ICE 597" in client.get("/").text
    r = client.get("/search", params={"q": "ice 597"}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == f"/train/{TRIP}"
    page = client.get(f"/train/{TRIP}")
    assert page.status_code == 200 and "Mannheim Hbf" in page.text
    data = client.get(f"/api/train/{TRIP}").json()
    ma = data["stops"][-1]
    assert (ma["station"], ma["db_delay"], ma["pred"], ma["pred_time"]) == (
        "Mannheim Hbf",
        6,
        7,
        "12:07",
    )
    assert data["stops"][0]["status"] == "passed" and data["next"]["stop_id"] == f"{TRIP}-5"
    assert client.get("/train/nope").status_code == 404


def test_replay_never_reads_ahead(raw: Path) -> None:
    write(raw, "fchg", MA, NOW + timedelta(minutes=10), fchg(MA, 5, "ar", "1215"))
    state = LiveState(raw, load_stations())
    state.refresh(NOW)
    assert state.stops[f"{TRIP}-5"].arr == datetime(2026, 11, 13, 12, 6)  # noqa: DTZ001
    state.refresh(NOW + timedelta(minutes=11))
    assert state.stops[f"{TRIP}-5"].arr == datetime(2026, 11, 13, 12, 15)  # noqa: DTZ001

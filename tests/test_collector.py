from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from dbdelay.api import TimetablesClient, plan_path
from dbdelay.collector import Collector
from dbdelay.config import Settings
from dbdelay.health import compute_health
from dbdelay.ratelimit import RateLimiter
from dbdelay.stations import Station
from dbdelay.storage import iter_raw_files, read_raw

FRANKFURT = Station("8000105", "Frankfurt(Main)Hbf", busy=True)
FULDA = Station("8000115", "Fulda", busy=False)
NOW = datetime(2026, 11, 6, 12, 0, tzinfo=UTC)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        db_client_id="id",  # type: ignore[arg-type]
        db_api_key="key",  # type: ignore[arg-type]
        data_dir=tmp_path,
        max_attempts=3,
        _env_file=None,  # type: ignore[call-arg]
    )


def _client(settings: Settings, handler: httpx.MockTransport) -> TimetablesClient:
    return TimetablesClient(settings, RateLimiter(6000, burst=100), transport=handler)


def test_plan_path_uses_berlin_time() -> None:
    slot = datetime(2026, 11, 6, 23, 0, tzinfo=UTC)  # 00:00 next day in Berlin
    assert plan_path("8000105", slot) == "/plan/8000105/261107/00"


async def test_poll_writes_raw_and_log(tmp_path: Path) -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"<timetable station='Frankfurt'/>")

    settings = _settings(tmp_path)
    async with _client(settings, httpx.MockTransport(handle)) as client:
        collector = Collector(settings, [FRANKFURT], client, clock=lambda: NOW)
        result = await collector.poll("fchg", FRANKFURT, NOW)

    assert result.ok and result.status == 200
    assert seen[0].headers["DB-Api-Key"] == "key"
    assert seen[0].url.path.endswith("/fchg/8000105")
    files = list(iter_raw_files(settings.raw_dir))
    assert len(files) == 1
    assert next(read_raw(files[0])).body == b"<timetable station='Frankfurt'/>"


async def test_retries_then_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503 if calls < 3 else 200, content=b"<timetable/>")

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", no_sleep)
    settings = _settings(tmp_path)
    async with _client(settings, httpx.MockTransport(handle)) as client:
        resp = await client.rchg("8000105")
    assert resp.status == 200 and resp.attempts == 3


async def test_auth_error_is_logged_not_retried(tmp_path: Path) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401, content=b"invalid key")

    settings = _settings(tmp_path)
    async with _client(settings, httpx.MockTransport(handle)) as client:
        collector = Collector(settings, [FRANKFURT], client, clock=lambda: NOW)
        result = await collector.poll("fchg", FRANKFURT, NOW)
    assert calls == 1
    assert not result.ok and result.status == 401
    assert result.error is not None and "invalid key" in result.error


async def test_health_counts_downtime_and_gaps(tmp_path: Path) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<timetable/>")

    settings = _settings(tmp_path)
    stations = [FRANKFURT, FULDA]
    async with _client(settings, httpx.MockTransport(handle)) as client:
        collector = Collector(settings, stations, client, clock=lambda: NOW)
        # One hour of fchg every 5 min, but Fulda is missing 12:20-12:40.
        for k in range(12):
            t = NOW + timedelta(minutes=5 * k)
            for st in stations:
                if st is FULDA and 4 <= k <= 7:
                    continue
                await collector.poll("fchg", st, t)
            for m in range(5):
                await collector.poll("rchg", FRANKFURT, t + timedelta(minutes=m))

    report = compute_health(settings, stations, NOW + timedelta(hours=1), hours=1)
    fchg = next(e for e in report.endpoints if e.endpoint == "fchg")
    rchg = next(e for e in report.endpoints if e.endpoint == "rchg")
    assert fchg.expected == 24 and fchg.ok == 20
    assert [g[0] for g in fchg.gaps] == ["8000115"]
    assert rchg.expected == 60 and rchg.uptime == 1.0
    assert report.uptime() == pytest.approx(80 / 84)

import asyncio
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


async def test_health_counts_from_first_start(tmp_path: Path) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<timetable/>")

    settings = _settings(tmp_path)
    started = NOW + timedelta(minutes=30)  # collector first ran halfway into the window
    async with _client(settings, httpx.MockTransport(handle)) as client:
        collector = Collector(settings, [FULDA], client, clock=lambda: started)
        for k in range(6):
            await collector.poll("fchg", FULDA, started + timedelta(minutes=5 * k))

    report = compute_health(settings, [FULDA], NOW + timedelta(hours=1), hours=1)
    fchg = next(e for e in report.endpoints if e.endpoint == "fchg")
    assert report.counted_from == started
    assert fchg.expected == 6 and fchg.uptime == 1.0
    assert fchg.gaps == []
    assert "counted from first start" in report.format()


async def test_health_still_counts_restart_downtime(tmp_path: Path) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<timetable/>")

    settings = _settings(tmp_path)
    async with _client(settings, httpx.MockTransport(handle)) as client:
        collector = Collector(settings, [FULDA], client, clock=lambda: NOW)
        # Runs 12:00-12:25, down until 12:45, then runs again.
        for k in [0, 1, 2, 3, 4, 5, 9, 10, 11]:
            await collector.poll("fchg", FULDA, NOW + timedelta(minutes=5 * k))

    report = compute_health(settings, [FULDA], NOW + timedelta(hours=1), hours=1)
    fchg = next(e for e in report.endpoints if e.endpoint == "fchg")
    assert report.counted_from == NOW
    assert fchg.expected == 12 and fchg.ok == 9
    assert len(fchg.gaps) == 1


async def test_plan_burst_does_not_delay_live_polls(tmp_path: Path) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<timetable/>")

    settings = _settings(tmp_path)
    limiter = RateLimiter(6000, burst=100)
    plan_limiter = RateLimiter(600, burst=1)  # 10/s
    async with TimetablesClient(
        settings, limiter, transport=httpx.MockTransport(handle), plan_limiter=plan_limiter
    ) as client:
        loop = asyncio.get_running_loop()
        start = loop.time()
        live_done: list[float] = []

        async def live() -> None:
            await client.fchg("8000105")
            live_done.append(loop.time() - start)

        plans = [client.plan("8000105", NOW + timedelta(hours=h)) for h in range(5)]
        await asyncio.gather(*plans, *(live() for _ in range(5)))
        total = loop.time() - start

    assert total >= 0.35  # 5 plan requests at 10/s with a burst of 1
    assert max(live_done) < 0.15  # live polls never waited behind them


async def test_stop_does_not_wait_for_slow_requests(tmp_path: Path) -> None:
    async def hang(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(3600)
        return httpx.Response(200, content=b"<timetable/>")

    settings = _settings(tmp_path)
    async with _client(settings, httpx.MockTransport(hang)) as client:
        collector = Collector(settings, [FRANKFURT, FULDA], client)
        asyncio.get_running_loop().call_later(0.2, collector.stop.set)
        await asyncio.wait_for(collector.run(), timeout=5)  # plan polls are still hanging


async def test_health_counts_each_slot_once(tmp_path: Path) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<timetable/>")

    settings = _settings(tmp_path)
    async with _client(settings, httpx.MockTransport(handle)) as client:
        collector = Collector(settings, [FULDA], client, clock=lambda: NOW)
        for k in range(12):
            t = NOW + timedelta(minutes=5 * k)
            await collector.poll("fchg", FULDA, t)
            if k < 6:  # a second collector logging the same slots
                await collector.poll("fchg", FULDA, t)

    report = compute_health(settings, [FULDA], NOW + timedelta(hours=1), hours=1)
    fchg = next(e for e in report.endpoints if e.endpoint == "fchg")
    assert fchg.ok == 12 and fchg.expected == 12

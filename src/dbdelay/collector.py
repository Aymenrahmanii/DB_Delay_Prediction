"""The always-on collector: /fchg every 5 min, /rchg every minute for busy stations, /plan hourly.

Polls are placed on a fixed wall-clock grid (multiples of the interval since the epoch, plus
a per-station offset), so a restart keeps the same schedule and the health check can count
how many polls were due in any window.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx

from dbdelay.api import Response, TimetablesClient
from dbdelay.config import BERLIN, Settings
from dbdelay.stations import Station
from dbdelay.storage import (
    Endpoint,
    PollLog,
    PollResult,
    RawRecord,
    append_raw,
    raw_file,
    read_raw,
)

log = logging.getLogger(__name__)

Clock = Callable[[], datetime]


def utcnow() -> datetime:
    return datetime.now(UTC)


def next_grid_time(now: datetime, interval_s: int, offset_s: float) -> datetime:
    """First time >= now on the grid {epoch + offset + k * interval}."""
    ts = now.timestamp() - offset_s
    k = -(-ts // interval_s)  # ceil
    return datetime.fromtimestamp(k * interval_s + offset_s, UTC)


def station_offsets(stations: list[Station], interval_s: int) -> dict[str, float]:
    """Spread stations evenly over one interval so requests don't all fire at once."""
    n = max(1, len(stations))
    return {s.eva: i * interval_s / n for i, s in enumerate(stations)}


def plan_slots(now: datetime, lookahead_hours: int) -> list[datetime]:
    """The current Berlin hour and the next `lookahead_hours`, as aware Berlin datetimes."""
    local = now.astimezone(BERLIN).replace(minute=0, second=0, microsecond=0)
    # Step in UTC so DST changes neither skip nor repeat an hour.
    base = local.astimezone(UTC)
    return [(base + timedelta(hours=h)).astimezone(BERLIN) for h in range(lookahead_hours + 1)]


class Collector:
    def __init__(
        self,
        settings: Settings,
        stations: list[Station],
        client: TimetablesClient,
        clock: Clock = utcnow,
    ) -> None:
        self.settings = settings
        self.stations = stations
        self.client = client
        self.clock = clock
        self.poll_log = PollLog(settings.poll_log_dir)
        self.stop = asyncio.Event()
        self.counts: Counter[tuple[str, bool]] = Counter()
        self._plan_done: set[tuple[str, str]] = set()

    # -- one poll ---------------------------------------------------------------------------

    async def poll(
        self,
        endpoint: Endpoint,
        station: Station,
        scheduled_at: datetime,
        slot: datetime | None = None,
    ) -> PollResult:
        resp: Response | None = None
        error: str | None = None
        written = 0
        try:
            if endpoint == "plan":
                assert slot is not None
                resp = await self.client.plan(station.eva, slot)
            elif endpoint == "fchg":
                resp = await self.client.fchg(station.eva)
            else:
                resp = await self.client.rchg(station.eva)
        except httpx.HTTPError as exc:
            error = f"{type(exc).__name__}: {exc}"

        fetched_at = self.clock()
        if resp is not None:
            record = RawRecord(
                endpoint=endpoint,
                eva=station.eva,
                path=resp.path,
                fetched_at=fetched_at.isoformat(),
                status=resp.status,
                body=resp.body,
            )
            partition = slot if slot is not None else fetched_at
            path = raw_file(self.settings.raw_dir, endpoint, station.eva, partition)
            written = await asyncio.to_thread(append_raw, path, record)
            if resp.status != 200:
                error = f"HTTP {resp.status}: {resp.body[:200].decode('utf-8', 'replace')}"

        result = PollResult(
            endpoint=endpoint,
            eva=station.eva,
            scheduled_at=scheduled_at.isoformat(),
            fetched_at=fetched_at.isoformat(),
            ok=resp is not None and resp.status == 200,
            status=resp.status if resp is not None else None,
            attempts=resp.attempts if resp is not None else 0,
            bytes=written,
            elapsed_s=resp.elapsed_s if resp is not None else 0.0,
            slot=slot.isoformat() if slot is not None else None,
            error=error,
        )
        self.poll_log.write(result, fetched_at)
        self.counts[(endpoint, result.ok)] += 1
        if not result.ok:
            log.warning("%s %s failed: %s", endpoint, station.name, error)
        return result

    # -- loops ------------------------------------------------------------------------------

    async def _sleep_until(self, when: datetime) -> bool:
        """Sleep until `when`; returns False if stop was requested meanwhile."""
        delay = (when - self.clock()).total_seconds()
        if delay > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop.wait(), timeout=delay)
        return not self.stop.is_set()

    async def periodic(
        self, endpoint: Endpoint, station: Station, interval_s: int, offset_s: float
    ) -> None:
        due = next_grid_time(self.clock(), interval_s, offset_s)
        while await self._sleep_until(due):
            try:
                await self.poll(endpoint, station, due)
            except Exception:  # never let one station's loop die
                log.exception("%s %s: unexpected error", endpoint, station.name)
            # Next grid point strictly after this one; if the poll overran (or the machine
            # slept), the points in between are skipped rather than fired in a burst.
            due = next_grid_time(
                max(self.clock(), due + timedelta(seconds=1)), interval_s, offset_s
            )

    def _plan_already_fetched(self, station: Station, slot: datetime) -> bool:
        key = (station.eva, slot.isoformat())
        if key in self._plan_done:
            return True
        path = raw_file(self.settings.raw_dir, "plan", station.eva, slot)
        if path.exists() and any(r.status == 200 for r in read_raw(path)):
            self._plan_done.add(key)
            return True
        return False

    async def plan_loop(self) -> None:
        interval = self.settings.plan_check_interval_s
        while not self.stop.is_set():
            now = self.clock()
            slots = plan_slots(now, self.settings.plan_lookahead_hours)
            todo = [
                (st, slot)
                for slot in slots
                for st in self.stations
                if not self._plan_already_fetched(st, slot)
            ]
            if todo:
                log.info("plan: fetching %d station-hours", len(todo))
            results = await asyncio.gather(
                *(self.poll("plan", st, now, slot) for st, slot in todo),
                return_exceptions=True,
            )
            for (st, slot), res in zip(todo, results, strict=True):
                if isinstance(res, PollResult) and res.ok:
                    self._plan_done.add((st.eva, slot.isoformat()))
                elif isinstance(res, BaseException):
                    log.error("plan %s %s: %r", st.name, slot, res)
            # Forget slots older than a day.
            cutoff = (now - timedelta(days=1)).isoformat()
            self._plan_done = {k for k in self._plan_done if k[1] >= cutoff}
            if not await self._sleep_until(next_grid_time(self.clock(), interval, 0)):
                return

    async def report_loop(self, every_s: int = 600) -> None:
        while await self._sleep_until(self.clock() + timedelta(seconds=every_s)):
            summary = ", ".join(
                f"{ep} {self.counts[(ep, True)]} ok/{self.counts[(ep, False)]} failed"
                for ep in ("plan", "fchg", "rchg")
            )
            log.info("last %ds: %s", every_s, summary)
            self.counts.clear()

    async def run(self) -> None:
        s = self.settings
        busy = [st for st in self.stations if st.busy]
        fchg_offsets = station_offsets(self.stations, s.fchg_interval_s)
        rchg_offsets = station_offsets(busy, s.rchg_interval_s)
        log.info(
            "collector starting: %d stations (%d busy), ~%.1f requests/min planned",
            len(self.stations),
            len(busy),
            expected_requests_per_minute(s, len(self.stations), len(busy)),
        )
        async with asyncio.TaskGroup() as tg:
            for st in self.stations:
                tg.create_task(self.periodic("fchg", st, s.fchg_interval_s, fchg_offsets[st.eva]))
            for st in busy:
                tg.create_task(self.periodic("rchg", st, s.rchg_interval_s, rchg_offsets[st.eva]))
            tg.create_task(self.plan_loop())
            tg.create_task(self.report_loop())
        log.info("collector stopped")


def expected_requests_per_minute(settings: Settings, n_stations: int, n_busy: int) -> float:
    fchg = n_stations * 60 / settings.fchg_interval_s
    rchg = n_busy * 60 / settings.rchg_interval_s
    plan = n_stations / 60  # steady state: one new hour per station per hour
    return fchg + rchg + plan


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError):  # Windows
            loop.add_signal_handler(sig, stop.set)

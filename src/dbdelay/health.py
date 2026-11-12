"""Collector health from the poll log: uptime against the schedule, and gaps.

Uptime counts polls that were *due* in the window from the configured schedule, not polls
that were logged, so time when the process was down counts as failure.
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import httpx

from dbdelay.config import Settings
from dbdelay.stations import Station
from dbdelay.storage import PollLog


@dataclass
class EndpointHealth:
    endpoint: str
    expected: int
    ok: int
    failed: int
    gaps: list[tuple[str, datetime, datetime]] = field(default_factory=list)

    @property
    def uptime(self) -> float:
        return min(1.0, self.ok / self.expected) if self.expected else 1.0


@dataclass
class HealthReport:
    start: datetime
    end: datetime
    counted_from: datetime
    endpoints: list[EndpointHealth]
    last_ok: datetime | None
    raw_bytes: int

    def uptime(self) -> float:
        expected = sum(e.expected for e in self.endpoints)
        ok = sum(min(e.ok, e.expected) for e in self.endpoints)
        return ok / expected if expected else 1.0

    def format(self) -> str:
        hours = (self.end - self.start).total_seconds() / 3600
        lines = [
            f"DB collector, last {hours:.0f} h (to {self.end:%Y-%m-%d %H:%M} UTC)",
            *(
                [f"counted from first start {self.counted_from:%Y-%m-%d %H:%M} UTC"]
                if self.counted_from > self.start
                else []
            ),
            f"uptime {self.uptime():.2%}, raw written {self.raw_bytes / 1e6:.1f} MB",
            f"last successful poll: {self.last_ok:%Y-%m-%d %H:%M:%S}"
            if self.last_ok
            else "last successful poll: none",
        ]
        for e in self.endpoints:
            lines.append(
                f"  {e.endpoint}: {e.ok}/{e.expected} ok ({e.uptime:.2%}), "
                f"{e.failed} failed, {len(e.gaps)} gaps"
            )
            for eva, a, b in e.gaps[:5]:
                lines.append(f"    gap {eva}: {a:%m-%d %H:%M} → {b:%m-%d %H:%M}")
            if len(e.gaps) > 5:
                lines.append(f"    … {len(e.gaps) - 5} more")
        return "\n".join(lines)


def _days(start: datetime, end: datetime) -> list[datetime]:
    days, d = [], start.replace(hour=0, minute=0, second=0, microsecond=0)
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


def compute_health(
    settings: Settings, stations: list[Station], end: datetime, hours: float
) -> HealthReport:
    start = end - timedelta(hours=hours)
    log = PollLog(settings.poll_log_dir)
    # Time before the collector ever ran is not downtime; restarts after it are.
    first = log.first_scheduled_at()
    counted_from = min(max(start, first), end) if first is not None else start
    window_s = (end - counted_from).total_seconds()
    busy = [s for s in stations if s.busy]
    intervals = {"fchg": settings.fchg_interval_s, "rchg": settings.rchg_interval_s}
    expected = {
        "fchg": int(window_s // settings.fchg_interval_s) * len(stations),
        "rchg": int(window_s // settings.rchg_interval_s) * len(busy),
    }

    ok: dict[str, int] = defaultdict(int)
    failed: dict[str, int] = defaultdict(int)
    ok_times: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    last_ok: datetime | None = None
    raw_bytes = 0

    seen: set[tuple[str, str, str, str | None]] = set()
    for row in log.read(_days(start, end)):
        t = datetime.fromisoformat(row["scheduled_at"])
        if not start <= t < end:
            continue
        ep = row["endpoint"]
        # Two collectors writing to one log would count every slot twice.
        slot_key = (ep, row["eva"], row["scheduled_at"], row.get("slot"))
        if slot_key in seen and ep != "plan":
            continue
        seen.add(slot_key)
        raw_bytes += int(row.get("bytes", 0))
        if row["ok"]:
            ok[ep] += 1
            ok_times[(ep, row["eva"])].append(t)
            fetched = datetime.fromisoformat(row["fetched_at"])
            last_ok = fetched if last_ok is None or fetched > last_ok else last_ok
        else:
            failed[ep] += 1

    endpoints: list[EndpointHealth] = []
    for ep, interval in intervals.items():
        h = EndpointHealth(ep, expected[ep], ok[ep], failed[ep])
        tolerance = timedelta(seconds=2 * interval)
        for st in stations if ep == "fchg" else busy:
            times = sorted(ok_times.get((ep, st.eva), []))
            points = [counted_from, *times, end]
            for a, b in itertools.pairwise(points):
                if b - a > tolerance:
                    h.gaps.append((st.eva, a, b))
        endpoints.append(h)
    # Plan polls are on demand, so they are reported without an expected count.
    endpoints.append(
        EndpointHealth("plan", ok["plan"] + failed["plan"], ok["plan"], failed["plan"])
    )
    return HealthReport(start, end, counted_from, endpoints, last_ok, raw_bytes)


def send_telegram(settings: Settings, text: str) -> None:
    token = settings.telegram_bot_token.get_secret_value()
    if not token or not settings.telegram_chat_id:
        raise RuntimeError("Telegram alert requested but DBDELAY_TELEGRAM_* is not set")
    resp = httpx.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": settings.telegram_chat_id, "text": text},
        timeout=20,
    )
    resp.raise_for_status()

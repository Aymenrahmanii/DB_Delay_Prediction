"""Command line: `dbdelay collect | health | stations-verify | archive-schema`."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
from collections.abc import Sequence

from lxml import etree

from dbdelay.api import TimetablesClient
from dbdelay.collector import Collector, install_signal_handlers, utcnow
from dbdelay.config import Settings
from dbdelay.health import compute_health, send_telegram
from dbdelay.lock import AlreadyRunningError, single_instance
from dbdelay.ratelimit import RateLimiter
from dbdelay.stations import load_stations

ARCHIVE_TABLES = (
    "monthly_processed_data",
    "monthly_processed_data_change",
    "monthly_processed_data_plan",
)


def _require_keys(settings: Settings) -> None:
    if not settings.db_client_id.get_secret_value() or not settings.db_api_key.get_secret_value():
        sys.exit("Set DBDELAY_DB_CLIENT_ID and DBDELAY_DB_API_KEY (see .env.example).")


async def _collect(settings: Settings) -> None:
    stations = load_stations(settings.stations_file)
    limiter = RateLimiter(settings.rate_limit_per_minute)
    plan_limiter = RateLimiter(settings.plan_rate_limit_per_minute, burst=2)
    async with TimetablesClient(settings, limiter, plan_limiter=plan_limiter) as client:
        collector = Collector(settings, stations, client)
        install_signal_handlers(collector.stop)
        await collector.run()


def cmd_collect(settings: Settings, _: argparse.Namespace) -> int:
    _require_keys(settings)
    try:
        with (
            single_instance(settings.data_dir / "collector.lock"),
            contextlib.suppress(KeyboardInterrupt),
        ):
            asyncio.run(_collect(settings))
    except AlreadyRunningError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


def cmd_health(settings: Settings, args: argparse.Namespace) -> int:
    stations = load_stations(settings.stations_file)
    report = compute_health(settings, stations, utcnow(), args.hours)
    text = report.format()
    print(text)
    healthy = report.uptime() >= args.min_uptime
    if args.alert == "always" or (args.alert == "on-fail" and not healthy):
        prefix = "OK" if healthy else "ALERT"
        send_telegram(settings, f"[{prefix}] {text}")
    return 0 if healthy else 1


async def _verify(settings: Settings) -> int:
    stations = load_stations(settings.stations_file)
    bad = 0
    async with TimetablesClient(settings, RateLimiter(settings.rate_limit_per_minute)) as client:
        for st in stations:
            resp = await client.station(st.eva)
            names: list[str] = []
            if resp.status == 200:
                root = etree.fromstring(resp.body)
                names = [
                    str(el.get("name")) for el in root.iter("station") if el.get("eva") == st.eva
                ]
            if names:
                flag = "" if names[0] == st.name else f"  (DB calls it {names[0]!r})"
                print(f"ok   {st.eva} {st.name}{flag}")
            else:
                bad += 1
                print(f"FAIL {st.eva} {st.name}: HTTP {resp.status}, EVA not found")
    print(f"{len(stations) - bad}/{len(stations)} stations verified")
    return 1 if bad else 0


def cmd_stations_verify(settings: Settings, _: argparse.Namespace) -> int:
    _require_keys(settings)
    return asyncio.run(_verify(settings))


def cmd_archive_schema(_: Settings, args: argparse.Namespace) -> int:
    """Print the columns of one month of each archive table, read remotely via DuckDB."""
    try:
        import duckdb
    except ImportError:
        sys.exit("Install the analysis group: uv sync --group analysis")
    con = duckdb.connect()
    for table in ARCHIVE_TABLES:
        url = f"hf://datasets/piebro/deutsche-bahn-data/{table}/data-{args.month}.parquet"
        print(f"== {table} ({args.month})")
        for name, typ, *_rest in con.execute(f"DESCRIBE SELECT * FROM '{url}'").fetchall():
            print(f"  {name:<32} {typ}")
        if args.sample:
            print(con.execute(f"SELECT * FROM '{url}' LIMIT {args.sample}").fetchdf().to_string())
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dbdelay")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("collect", help="run the collector until stopped").set_defaults(fn=cmd_collect)

    p = sub.add_parser("health", help="uptime and gaps from the poll log")
    p.add_argument("--hours", type=float, default=24)
    p.add_argument("--min-uptime", type=float, default=0.99)
    p.add_argument("--alert", choices=["never", "on-fail", "always"], default="never")
    p.set_defaults(fn=cmd_health)

    sub.add_parser("stations-verify", help="check every EVA number against /station").set_defaults(
        fn=cmd_stations_verify
    )

    p = sub.add_parser("archive-schema", help="columns of the Hugging Face archive tables")
    p.add_argument("--month", default="2026-10")
    p.add_argument("--sample", type=int, default=0)
    p.set_defaults(fn=cmd_archive_schema)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    code: int = args.fn(Settings(), args)
    return code


if __name__ == "__main__":
    raise SystemExit(main())

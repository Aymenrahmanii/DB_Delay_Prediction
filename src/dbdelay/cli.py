"""Command line: `dbdelay collect | health | build-tables | trip | check | ...`."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
import time
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
from lxml import etree

from dbdelay.api import TimetablesClient
from dbdelay.awake import keep_awake
from dbdelay.collector import Collector, install_signal_handlers, utcnow
from dbdelay.config import BERLIN, Settings
from dbdelay.health import compute_health, send_telegram
from dbdelay.lock import AlreadyRunningError, single_instance
from dbdelay.ratelimit import RateLimiter
from dbdelay.stations import load_stations

log = logging.getLogger("dbdelay")

ARCHIVE_TABLES = (
    "monthly_processed_data",
    "monthly_processed_data_change",
    "monthly_processed_data_plan",
)


def _require_keys(settings: Settings) -> None:
    if not settings.db_client_id.get_secret_value() or not settings.db_api_key.get_secret_value():
        sys.exit("Set DBDELAY_DB_CLIENT_ID and DBDELAY_DB_API_KEY (see .env.example).")


async def _collect(settings: Settings, hours: float | None) -> None:
    stations = load_stations(settings.stations_file)
    limiter = RateLimiter(settings.rate_limit_per_minute)
    plan_limiter = RateLimiter(settings.plan_rate_limit_per_minute, burst=2)
    async with TimetablesClient(settings, limiter, plan_limiter=plan_limiter) as client:
        collector = Collector(settings, stations, client)
        install_signal_handlers(collector.stop)
        if hours is not None:
            asyncio.get_running_loop().call_later(hours * 3600, collector.stop.set)
            log.info("will stop after %g h", hours)
        await collector.run()


def cmd_collect(settings: Settings, args: argparse.Namespace) -> int:
    _require_keys(settings)
    try:
        with (
            single_instance(settings.data_dir / "collector.lock"),
            keep_awake() if args.keep_awake else contextlib.nullcontext(False) as awake,
            contextlib.suppress(KeyboardInterrupt),
        ):
            if args.keep_awake:
                log.info("keeping the PC awake: %s", "yes" if awake else "not supported here")
            asyncio.run(_collect(settings, args.hours))
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


def _tables_dir(settings: Settings) -> Path:
    return settings.data_dir / "tables"


def cmd_build_tables(settings: Settings, _: argparse.Namespace) -> int:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        sys.exit("Install the analysis group: uv sync --group analysis")
    from dbdelay.tables import build_tables

    started = time.monotonic()
    stats = build_tables(settings.raw_dir, _tables_dir(settings))
    print(
        f"plan responses {stats.plan_records}, change responses {stats.change_records}, "
        f"unparseable {stats.bad_records}\n"
        f"planned long-distance stops {stats.planned_stops}, change events "
        f"{stats.change_events} (repeats dropped: {stats.repeated_fetches}, "
        f"same-instant conflicts: {stats.same_instant_conflicts})\n"
        f"days {', '.join(stats.days)}; wrote {_tables_dir(settings)} "
        f"in {time.monotonic() - started:.0f} s"
    )
    return 0


def _fmt_time(t: datetime | None) -> str:
    return f"{t:%H:%M}" if t else "  -  "


def _fmt_delay(d: float | None) -> str:
    return f"{d:+4.0f}" if d is not None else "    "


def cmd_trip(settings: Settings, args: argparse.Namespace) -> int:
    from dbdelay import stops as st
    from dbdelay.archive import archive_url, connect

    if args.date is None:
        args.date = utcnow().astimezone(BERLIN).date().isoformat()
    if args.source == "archive":
        source = args.archive or archive_url(args.date[:7])
        con = connect(source)
        sql = st.archive_stops_sql(source, long_distance_only=False)
    else:
        con = duckdb.connect()
        sql = st.collector_stops_sql(
            _tables_dir(settings), load_stations(settings.stations_file), long_distance_only=False
        )
    if args.trip_key:
        keys = [args.trip_key]
    else:
        trips = st.find_trips(con, sql, args.train, args.date)
        if not trips:
            print(f"no trip of {args.train} on {args.date} in the {args.source} data")
            return 1
        if len(trips) > 1:
            print(f"{len(trips)} trips of {args.train} on {args.date}:")
            for t in trips:
                print(
                    f"  {t['trip_key']}  {t['first_planned']:%H:%M}-{t['last_planned']:%H:%M}"
                    f"  {t['stops']} stops"
                )
        keys = [t["trip_key"] for t in trips]
    for key in keys:
        rows = st.trip_stops(con, sql, key)
        if not rows:
            print(f"no stops for trip {key}")
            continue
        r0 = rows[0]
        print(f"\n{r0['train_type']} {r0['train_number']}  trip {key}  ({args.source})")
        print(" #  station                       arr  changed delay   dep  changed delay  notes")
        for r in rows:
            flags = (
                ("cancelled", r["canceled"]),
                ("added", r["is_added"]),
                ("diversion", r["is_diversion"]),
                ("platform changed", r["platform_changed"]),
                ("not observed", r.get("observed") is False),
            )
            notes = ", ".join(n for n, flag in flags if flag)
            print(
                f"{r['stop_num']:>2}  {(r['station_name'] or r['eva'])[:28]:<28}  "
                f"{_fmt_time(r['arr_planned'])}  {_fmt_time(r['arr_changed'])}  "
                f"{_fmt_delay(r['arr_delay_min'])}   {_fmt_time(r['dep_planned'])}  "
                f"{_fmt_time(r['dep_changed'])}  {_fmt_delay(r['dep_delay_min'])}  {notes}"
            )
    return 0


def cmd_check(settings: Settings, args: argparse.Namespace) -> int:
    from dbdelay.checks import check_day

    if args.date:
        day = date.fromisoformat(args.date)
    else:
        day = utcnow().astimezone(BERLIN).date() - timedelta(days=1)
    stations = load_stations(settings.stations_file)
    results = check_day(duckdb.connect(), settings, stations, _tables_dir(settings), day)
    print(f"checks for {day}")
    for r in results:
        print("  " + r.format())
    return 1 if any(not r.ok and r.level == "error" for r in results) else 0


def cmd_archive_pull(settings: Settings, args: argparse.Namespace) -> int:
    from dbdelay.archive import pull_month

    evas = [s.eva for s in load_stations(settings.stations_file)]
    for month in args.months:
        started = time.monotonic()
        out, n = pull_month(month, evas, settings.data_dir / "archive")
        print(f"{month}: {n} stops -> {out} ({time.monotonic() - started:.0f} s)")
    return 0


def cmd_train(settings: Settings, args: argparse.Namespace) -> int:
    from dbdelay import baselines as bl
    from dbdelay import model as m
    from dbdelay.features import FEATURES

    archive_dir = settings.data_dir / "archive"
    train_files = [archive_dir / f"stops-{mo}.parquet" for mo in args.months]
    history = sorted(archive_dir.glob("stops-*.parquet"))
    missing = [f for f in train_files if not f.exists()]
    if missing:
        sys.exit(f"missing {', '.join(map(str, missing))}; run dbdelay archive-pull first")
    stations = load_stations(settings.stations_file)
    valid_from = date.fromisoformat(args.valid_from)

    started = time.monotonic()
    booster, info = m.train(duckdb.connect(), stations, train_files, history, valid_from)
    print(
        f"trained on {info['rows_train']} rows, validated on {info['rows_valid']} "
        f"(from {valid_from}): MAE {info['valid_mae']:.2f} min at iteration "
        f"{info['best_iteration']} ({time.monotonic() - started:.0f} s)"
    )
    paths = bl.Paths(_tables_dir(settings), settings.poll_log_dir)
    test = m.score_on_collector(duckdb.connect(), booster, stations, paths, history)

    print("\ncollected days, mean absolute error of the arrival delay, minutes (same rows)")
    print("horizon       n  persistence      DB   model   model vs DB")
    for r in test:
        gain = 1 - r["mae_model"] / r["mae_db"]
        print(
            f"{r['horizon']:>4} min {r['n']:>7}       {_f(r['mae_persistence'])}  "
            f"{_f(r['mae_db'])}  {_f(r['mae_model'])}   {gain:+.1%}"
        )
    record = m.RunRecord(
        run_id=m.new_run_id(),
        train_files=[f.name for f in train_files],
        valid_from=valid_from.isoformat(),
        rows_train=info["rows_train"],
        rows_valid=info["rows_valid"],
        rows_test=sum(r["n"] for r in test),
        features=list(FEATURES),
        params=m.PARAMS,
        best_iteration=info["best_iteration"],
        valid_mae=info["valid_mae"],
        test=test,
    )
    print(f"\nrun recorded in {m.save_run(settings.data_dir / 'runs', booster, record)}")
    return 0


def _f(v: float | None) -> str:
    return f"{v:6.2f}" if v is not None else "     -"


def cmd_baselines(settings: Settings, args: argparse.Namespace) -> int:
    from dbdelay import baselines as bl

    con = duckdb.connect()
    paths = bl.Paths(_tables_dir(settings), settings.poll_log_dir)
    bl.build(con, paths, load_stations(settings.stations_file), basis=args.basis)

    a = bl.actual_stats(con)
    print(
        f"target stops {a['stops']}: actual known {a['with_actual']}, cancelled "
        f"{a['cancelled']}, not covered {a['not_covered']}; DB moved the time after the "
        f"last word for {a['moved_after_share'] or 0:.1%}"
    )
    print("\nsnapshots per horizon (running = train already left a collected stop):")
    for c in bl.coverage(con):
        print(f"  {c['horizon']:>3} min  {c['snapshots']:>6}  running {c['running']:>6}")

    print(
        f"\nmean absolute error of the arrival delay, minutes; horizon back from the "
        f"{args.basis} arrival; same rows for all three"
    )
    print("horizon       n  stops  timetable  persistence      DB  mean delay")
    for r in bl.score(con):
        print(
            f"{r['horizon']:>4} min {r['n']:>7} {r['stops']:>6}     {_f(r['mae_timetable'])}"
            f"       {_f(r['mae_persistence'])}  {_f(r['mae_db'])}      {_f(r['mean_delay'])}"
        )
    if args.by_hour:
        print("\nby hour of planned arrival")
        for r in bl.score(con, by="horizon, hour"):
            print(
                f"{r['horizon']:>4} min {r['hour']:02d}h {r['n']:>6}  {_f(r['mae_timetable'])}"
                f"  {_f(r['mae_persistence'])}  {_f(r['mae_db'])}"
            )
    out = _tables_dir(settings) / "snapshots.parquet"
    con.execute(f"copy snapshots to '{out.as_posix()}' (format parquet)")
    print(f"\nwrote {out}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dbdelay")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--log-file", type=Path, help="append logs to this file (UTF-8) instead of stderr"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("collect", help="run the collector until stopped")
    p.add_argument("--hours", type=float, help="stop on its own after this many hours")
    p.add_argument("--keep-awake", action="store_true", help="block sleep while running (Windows)")
    p.set_defaults(fn=cmd_collect)

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

    sub.add_parser("build-tables", help="parse the raw archive into Parquet tables").set_defaults(
        fn=cmd_build_tables
    )

    p = sub.add_parser("archive-pull", help="save archive months for the 40 stations locally")
    p.add_argument("months", nargs="+", help="YYYY-MM")
    p.set_defaults(fn=cmd_archive_pull)

    p = sub.add_parser("train", help="train the median model on the archive, score on collected")
    p.add_argument("--months", nargs="+", default=["2026-07", "2026-08", "2026-09", "2026-10"])
    p.add_argument("--valid-from", default="2026-10-18", help="first day of validation")
    p.set_defaults(fn=cmd_train)

    p = sub.add_parser("baselines", help="actual times, snapshots and baseline errors")
    p.add_argument("--by-hour", action="store_true", help="also break errors down by hour")
    p.add_argument(
        "--basis",
        choices=["actual", "planned"],
        default="actual",
        help="measure the horizon back from the actual or the planned arrival",
    )
    p.set_defaults(fn=cmd_baselines)

    p = sub.add_parser("trip", help="one trip stop by stop, planned and changed times")
    p.add_argument("train", nargs="?", default="", help='e.g. "ICE 597"')
    p.add_argument("--date", help="YYYY-MM-DD (Berlin); default today")
    p.add_argument("--trip-key", help="exact trip key instead of train and date")
    p.add_argument("--source", choices=["collector", "archive"], default="collector")
    p.add_argument("--archive", help="archive Parquet path or URL (default: the month's file)")
    p.set_defaults(fn=cmd_trip)

    p = sub.add_parser("check", help="data checks on one day of the collector tables")
    p.add_argument("--date", help="YYYY-MM-DD (Berlin); default yesterday")
    p.set_defaults(fn=cmd_check)

    args = parser.parse_args(argv)
    if args.log_file is not None:
        args.log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        filename=args.log_file,
        encoding="utf-8",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        code: int = args.fn(Settings(), args)
    except Exception:
        # With --log-file nobody sees stderr, so a crash must land in the log too.
        logging.getLogger("dbdelay").exception("%s failed", args.command)
        raise
    return code


if __name__ == "__main__":
    raise SystemExit(main())

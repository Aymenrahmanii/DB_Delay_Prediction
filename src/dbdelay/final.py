"""Week 8: freeze the model, then score fresh days once.

``freeze`` records the model run to publish in ``frozen.json`` (committed to git, so the
freeze has a date anyone can check): its id, a SHA-256 of every model file, and the first day
of the final test, the day after the freeze. Nothing about the model may change after that.

``final`` scores the frozen models on the days from that first test day on, with the same
definitions as the baselines (forecasts made 15/30/60/90 min before the actual arrival, all
compared on the same rows). It checks the fingerprints first, refuses days before the test
window, and writes ``results/final.json`` and ``results/final.md``. It runs once: a second run
needs ``force`` and is recorded as such.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from dbdelay import baselines as bl
from dbdelay.model import QUANTILES, collector_predictions, load_run
from dbdelay.stations import Station
from dbdelay.stops import query

FROZEN_FILE = Path("frozen.json")  # in the repository: the freeze is in git history
BOOTSTRAP = 2000
LATE_MIN = 5


@dataclass
class Frozen:
    run_id: str
    frozen_at: str
    test_from: str
    files: dict[str, str]  # file name -> sha256
    note: str = ""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_files(runs_dir: Path, run_id: str) -> list[Path]:
    record = json.loads((runs_dir / f"{run_id}.json").read_text(encoding="utf-8"))
    files = [runs_dir / f"{run_id}.json"]
    for name in record.get("models", {}):
        files += [runs_dir / f"{run_id}.{name}.q{int(q * 100)}.lgb.txt" for q in QUANTILES]
    return files


def freeze(runs_dir: Path, run_id: str, out: Path, today: date, note: str = "") -> Frozen:
    if out.exists():
        raise FileExistsError(f"{out} exists: the model is already frozen")
    frozen = Frozen(
        run_id=run_id,
        frozen_at=datetime.now(UTC).isoformat(timespec="seconds"),
        test_from=(today + timedelta(days=1)).isoformat(),
        files={f.name: _sha256(f) for f in run_files(runs_dir, run_id)},
        note=note,
    )
    out.write_text(json.dumps(asdict(frozen), indent=2) + "\n", encoding="utf-8")
    return frozen


def load_frozen(path: Path, runs_dir: Path) -> Frozen:
    """Reads ``frozen.json`` and checks every model file against its fingerprint."""
    frozen = Frozen(**json.loads(path.read_text(encoding="utf-8")))
    for name, digest in frozen.files.items():
        f = runs_dir / name
        if not f.exists():
            raise FileNotFoundError(f"frozen model file missing: {f}")
        if _sha256(f) != digest:
            raise ValueError(f"{f} does not match the frozen fingerprint")
    return frozen


@dataclass
class FinalResult:
    run_id: str
    test_from: str
    test_to: str
    days: list[str]
    scored_at: str
    forced: bool
    horizons: list[dict[str, Any]] = field(default_factory=list)
    by_day: list[dict[str, Any]] = field(default_factory=list)
    late: list[dict[str, Any]] = field(default_factory=list)


def _bootstrap_gain(rows: list[tuple[str, float, float]], seed: int = 7) -> tuple[float, float]:
    """95 % interval of 1 - MAE(model)/MAE(DB), resampling whole trips."""
    import numpy as np

    by_trip: dict[str, list[float]] = {}
    for trip, e_db, e_m in rows:
        acc = by_trip.setdefault(trip, [0.0, 0.0])
        acc[0] += e_db
        acc[1] += e_m
    sums = np.array(list(by_trip.values()))
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(sums), size=(BOOTSTRAP, len(sums)))
    tot = sums[idx].sum(axis=1)
    gains = 1 - tot[:, 1] / tot[:, 0]
    lo, hi = np.percentile(gains, [2.5, 97.5])
    return float(lo), float(hi)


def final_scores(
    frozen: Frozen,
    runs_dir: Path,
    stations: list[Station],
    paths: bl.Paths,
    history: list[Path],
    test_to: date,
    forced: bool = False,
) -> FinalResult:
    _, models = load_run(runs_dir, frozen.run_id)
    con = collector_predictions(models, stations, paths, history)
    start = date.fromisoformat(frozen.test_from)
    con.execute(
        f"""create or replace temp table final as select * from scored
            where cast(arr_planned as date) between date '{start}' and date '{test_to}'"""
    )
    days = [
        str(r[0])
        for r in con.execute(
            "select distinct cast(arr_planned as date) from final order by 1"
        ).fetchall()
    ]
    if not days:
        raise ValueError(f"no scored snapshots between {start} and {test_to}: collect first")
    result = FinalResult(
        frozen.run_id,
        start.isoformat(),
        test_to.isoformat(),
        days,
        datetime.now(UTC).isoformat(timespec="seconds"),
        forced,
    )
    result.horizons = query(
        con,
        """select model, horizon, count(*) as n, count(distinct trip_key) as trips,
                  avg(abs(actual_delay - pers_delay)) as mae_persistence,
                  avg(abs(actual_delay - db_delay)) as mae_db,
                  avg(abs(actual_delay - pred)) as mae_model,
                  avg((actual_delay between lo and hi)::int) as coverage
           from final group by all order by all""",
    )
    for r in result.horizons:
        rows = con.execute(
            """select trip_key, abs(actual_delay - db_delay), abs(actual_delay - pred)
               from final where model = ? and horizon = ?""",
            [r["model"], r["horizon"]],
        ).fetchall()
        r["gain_vs_db"] = 1 - r["mae_model"] / r["mae_db"]
        r["gain_ci_low"], r["gain_ci_high"] = _bootstrap_gain(rows)
        r["gain_vs_persistence"] = 1 - r["mae_model"] / r["mae_persistence"]
    result.by_day = query(
        con,
        """select cast(arr_planned as date)::varchar as day, model, horizon, count(*) as n,
                  avg(abs(actual_delay - db_delay)) as mae_db,
                  avg(abs(actual_delay - pred)) as mae_model
           from final group by all order by all""",
    )
    result.late = query(
        con,
        f"""select model, horizon,
               count(*) filter (where pred > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where pred > {LATE_MIN}), 0) as precision_model,
               count(*) filter (where pred > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where actual_delay > {LATE_MIN}), 0) as recall_model,
               count(*) filter (where db_delay > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where db_delay > {LATE_MIN}), 0) as precision_db,
               count(*) filter (where db_delay > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where actual_delay > {LATE_MIN}), 0) as recall_db
           from final group by all order by all""",
    )
    return result


def markdown(result: FinalResult) -> str:
    """The README's results table."""
    lines = [
        f"Final test: {result.test_from} to {result.test_to} ({len(result.days)} days), "
        f"frozen model run `{result.run_id}`, scored once on {result.scored_at[:10]}"
        + (" (re-run with --force)" if result.forced else "")
        + ".",
        "",
        "| Horizon | Forecasts | Persistence | DB's forecast | Model | vs DB (95 % CI) "
        "| vs persistence | 80 % range covers |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in result.horizons:
        if r["model"] != "with_db":
            continue
        lines.append(
            f"| {r['horizon']} min | {r['n']:,} | {r['mae_persistence']:.2f} | "
            f"{r['mae_db']:.2f} | {r['mae_model']:.2f} | {r['gain_vs_db']:+.1%} "
            f"({r['gain_ci_low']:+.1%}, {r['gain_ci_high']:+.1%}) | "
            f"{r['gain_vs_persistence']:+.1%} | {r['coverage']:.1%} |"
        )
    lines += ["", "Without DB's forecast as input:", ""]
    lines += [
        f"- {r['horizon']} min: {r['mae_model']:.2f} min ({r['gain_vs_db']:+.1%} vs DB)"
        for r in result.horizons
        if r["model"] == "without_db"
    ]
    return "\n".join(lines) + "\n"


def write_result(results_dir: Path, result: FinalResult) -> tuple[Path, Path]:
    results_dir.mkdir(parents=True, exist_ok=True)
    j, m = results_dir / "final.json", results_dir / "final.md"
    j.write_text(json.dumps(asdict(result), indent=2, default=str) + "\n", encoding="utf-8")
    m.write_text(markdown(result), encoding="utf-8")
    return j, m

"""Score a day of logged forecasts against what happened (the nightly scoreboard).

A forecast is scored at horizon h if it was made h minutes (± ``MATCH_MIN``) before the
stop's actual arrival, the definition used for the baselines; per stop, model and horizon
the forecast made closest to that moment counts. Actual arrivals come from ``baselines``
(DB's last word before the arrival, only where the collector was polling). On the same rows:
the model's median, DB's forecast as it stood when the model's forecast was made, and
persistence (the delay the train had then). Results go to ``data/scores/<source>/<day>.json``.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from dbdelay import baselines as bl
from dbdelay.stations import Station
from dbdelay.stops import literal, query

MATCH_MIN = 2.5
LATE_MIN = 5  # "late" for precision and recall


@dataclass
class DayScore:
    day: str
    source: str
    run_ids: list[str]
    forecasts: int  # forecasts logged that day
    rows: list[dict[str, Any]] = field(default_factory=list)  # per model and horizon


def score_day(
    paths: bl.Paths, stations: list[Station], predictions: Path, day: date, source: str
) -> DayScore:
    con = duckdb.connect()
    bl.build(con, paths, stations)
    con.execute(
        f"""
        create temp table preds as
        select * from read_json_auto({literal(predictions.as_posix())})
        where cast(made_at as date) = date {literal(day.isoformat())}
        """
    )
    forecasts = con.execute("select count(*) from preds").fetchone()
    hs = ", ".join(f"({h})" for h in bl.HORIZONS_MIN)
    con.execute(
        f"""
        create temp table matched as
        select p.*, a.arr_actual,
               date_diff('second', a.arr_planned, a.arr_actual) / 60.0 as actual_delay,
               h.h as horizon
        from preds p
        join act a using (stop_id)
        cross join (values {hs}) h(h)
        where a.covered and not a.canceled and a.arr_actual is not null
          and abs(date_diff('second', cast(p.made_at as timestamp), a.arr_actual) / 60.0 - h.h)
              <= {MATCH_MIN}
        qualify row_number() over (
            partition by p.stop_id, p.model, h.h
            order by abs(date_diff('second', cast(p.made_at as timestamp), a.arr_actual)
                         / 60.0 - h.h)
        ) = 1
        """
    )
    rows = query(
        con,
        f"""
        select model, horizon, count(*) as n,
               avg(abs(actual_delay - pred_delay)) as mae_model,
               avg(abs(actual_delay - db_delay)) as mae_db,
               avg(abs(actual_delay - cur_delay)) as mae_persistence,
               avg((actual_delay between lo and hi)::int) as coverage,
               avg(hi - lo) as width,
               count(*) filter (where actual_delay > {LATE_MIN}) as late,
               count(*) filter (where pred_delay > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where pred_delay > {LATE_MIN}), 0)
                   as precision_model,
               count(*) filter (where pred_delay > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where actual_delay > {LATE_MIN}), 0)
                   as recall_model,
               count(*) filter (where db_delay > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where db_delay > {LATE_MIN}), 0) as precision_db,
               count(*) filter (where db_delay > {LATE_MIN} and actual_delay > {LATE_MIN})
                   / nullif(count(*) filter (where actual_delay > {LATE_MIN}), 0) as recall_db
        from matched group by all order by all
        """,
    )
    run_ids = (
        [
            r[0]
            for r in con.execute(
                "select distinct run_id from preds where run_id is not null order by 1"
            ).fetchall()
        ]
        if "run_id" in [c[0] for c in con.execute("describe preds").fetchall()]
        else []
    )
    return DayScore(day.isoformat(), source, run_ids, int(forecasts[0]) if forecasts else 0, rows)


def save_score(scores_dir: Path, score: DayScore) -> Path:
    out = scores_dir / score.source / f"{score.day}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(asdict(score), indent=2, default=str), encoding="utf-8")
    return out


def load_scores(scores_dir: Path, source: str) -> list[DayScore]:
    out = []
    for f in sorted((scores_dir / source).glob("*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        out.append(DayScore(**d))
    return out


def totals(scores: list[DayScore], model: str) -> list[dict[str, Any]]:
    """All days together, per horizon, weighted by the number of scored forecasts."""
    acc: dict[int, dict[str, float]] = {}
    for s in scores:
        for r in s.rows:
            if r["model"] != model:
                continue
            a = acc.setdefault(
                int(r["horizon"]), {"n": 0, "model": 0, "db": 0, "pers": 0, "cov": 0}
            )
            n = r["n"]
            a["n"] += n
            a["model"] += r["mae_model"] * n
            a["db"] += r["mae_db"] * n
            a["pers"] += r["mae_persistence"] * n
            a["cov"] += r["coverage"] * n
    return [
        {
            "horizon": h,
            "n": int(a["n"]),
            "mae_model": a["model"] / a["n"],
            "mae_db": a["db"] / a["n"],
            "mae_persistence": a["pers"] / a["n"],
            "coverage": a["cov"] / a["n"],
            "gain_vs_db": 1 - a["model"] / a["db"] if a["db"] else None,
        }
        for h, a in sorted(acc.items())
        if a["n"]
    ]

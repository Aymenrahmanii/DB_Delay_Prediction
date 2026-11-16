"""Week 4: a LightGBM median model trained on the archive, scored on the collected days.

Split by date, never randomly: training rows before ``valid_from``, validation rows (early
stopping) from it to the end of the archive, and the test set is the collector's snapshots,
which no model step sees. Each run writes ``data/runs/<run id>.json`` (data window, features,
parameters, metrics) and the model next to it.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from dbdelay import baselines as bl
from dbdelay.features import CATEGORICAL, FEATURES, archive_inputs, build_features, collector_inputs
from dbdelay.stations import Station
from dbdelay.stops import query

PARAMS: dict[str, Any] = {
    "objective": "l1",  # the median: robust to the long tail of big delays
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_data_in_leaf": 200,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "verbose": -1,
    "seed": 7,
}


@dataclass
class RunRecord:
    run_id: str
    train_files: list[str]
    valid_from: str
    rows_train: int
    rows_valid: int
    rows_test: int
    features: list[str]
    params: dict[str, Any]
    best_iteration: int
    valid_mae: float
    test: list[dict[str, Any]] = field(default_factory=list)


def _matrix(con: duckdb.DuckDBPyConnection, where: str) -> tuple[Any, Any, Any]:
    import polars as pl

    cols = ", ".join(f"cast({f} as double) as {f}" for f in FEATURES)
    df = con.sql(
        f"select stop_id, horizon, actual_delay, {cols} from features where {where}"
    ).to_arrow_table()
    df = pl.DataFrame(df)
    x = df.select(FEATURES).to_numpy()
    y = df.get_column("actual_delay").cast(pl.Float64).to_numpy()
    return x, y, df.select("stop_id", "horizon")


def train(
    con: duckdb.DuckDBPyConnection,
    stations: list[Station],
    train_files: list[Path],
    history_files: list[Path],
    valid_from: date,
) -> tuple[Any, dict[str, Any]]:
    import lightgbm as lgb

    archive_inputs(con, train_files)
    build_features(con, stations, history_files)
    v = f"date '{valid_from.isoformat()}'"
    x_tr, y_tr, _ = _matrix(con, f"t < {v}")
    x_va, y_va, _ = _matrix(con, f"t >= {v}")
    cats = [FEATURES.index(c) for c in CATEGORICAL]
    d_tr = lgb.Dataset(x_tr, y_tr, feature_name=list(FEATURES), categorical_feature=cats)
    d_va = lgb.Dataset(x_va, y_va, reference=d_tr)
    booster = lgb.train(
        PARAMS,
        d_tr,
        num_boost_round=3000,
        valid_sets=[d_va],
        callbacks=[lgb.early_stopping(100, verbose=False)],
    )
    info = {
        "rows_train": len(y_tr),
        "rows_valid": len(y_va),
        "best_iteration": booster.best_iteration,
        "valid_mae": float(booster.best_score["valid_0"]["l1"]),
    }
    return booster, info


def score_on_collector(
    con: duckdb.DuckDBPyConnection,
    booster: Any,
    stations: list[Station],
    paths: bl.Paths,
    history_files: list[Path],
) -> list[dict[str, Any]]:
    """MAE by horizon of the model, DB and persistence, on the same snapshot rows."""
    import polars as pl

    bl.build(con, paths, stations)
    collector_inputs(con)
    build_features(con, stations, history_files)
    x, _, ids = _matrix(con, "true")
    preds = ids.with_columns(
        pred=pl.Series(booster.predict(x, num_iteration=booster.best_iteration))
    )
    con.register("preds", preds.to_arrow())
    return query(
        con,
        """
        select s.horizon, count(*) as n,
               avg(abs(s.actual_delay - s.pers_delay)) as mae_persistence,
               avg(abs(s.actual_delay - s.db_delay)) as mae_db,
               avg(abs(s.actual_delay - p.pred)) as mae_model,
               avg(s.actual_delay - p.pred) as model_bias,
               avg(s.actual_delay - s.db_delay) as db_bias
        from snapshots s join preds p using (stop_id, horizon)
        where s.pers_delay is not null
        group by s.horizon order by s.horizon
        """,
    )


def save_run(runs_dir: Path, booster: Any, record: RunRecord) -> Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(runs_dir / f"{record.run_id}.lgb.txt"))
    out = runs_dir / f"{record.run_id}.json"
    out.write_text(json.dumps(asdict(record), indent=2, default=str), encoding="utf-8")
    return out


def new_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

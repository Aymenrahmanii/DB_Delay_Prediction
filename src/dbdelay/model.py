"""LightGBM quantile models trained on the archive, scored on the collected days.

Two models, each one LightGBM per quantile (10 %, 50 %, 90 %); the median is the forecast,
10-90 % the 80 % range:

- ``without_db``: what can be learned from delays alone, trained on moments every 15 min;
- ``with_db``: the same plus DB's forecast for the target at t, trained on the archive's
  snapshot moments, the only times its DB forecast is known.

Split by date, never randomly: training rows before ``valid_from``, validation rows (early
stopping and range calibration) from it to the end of the archive. The test set is the
collector's snapshots, which no step sees. The range is calibrated with conformalized
quantile regression on the validation rows: widened (or narrowed) by the amount that makes
it cover 80 % there. Each run writes ``data/runs/<run id>.json`` and the models next to it.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from dbdelay import baselines as bl
from dbdelay.features import (
    CATEGORICAL,
    FEATURES,
    FEATURES_DB,
    archive_db_inputs,
    archive_inputs,
    build_features,
    collector_inputs,
)
from dbdelay.stations import Station
from dbdelay.stops import query

QUANTILES = (0.1, 0.5, 0.9)
COVERAGE = 0.8
PARAMS: dict[str, Any] = {
    "learning_rate": 0.08,
    "num_leaves": 63,
    "min_data_in_leaf": 200,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "verbose": -1,
    "seed": 7,
}


@dataclass
class QuantileModel:
    name: str
    features: tuple[str, ...]
    boosters: dict[float, Any]
    widen: float  # minutes added on both sides of the 10-90 % range (conformal)
    info: dict[str, Any]

    def predict(self, x: Any) -> tuple[Any, Any, Any]:
        """Median and calibrated 80 % range; the raw quantiles are sorted so they never
        cross."""
        import numpy as np

        raw = np.sort(
            np.column_stack(
                [
                    self.boosters[q].predict(x, num_iteration=self.boosters[q].best_iteration)
                    for q in QUANTILES
                ]
            ),
            axis=1,
        )
        return raw[:, 1], raw[:, 0] - self.widen, raw[:, 2] + self.widen


def _matrix(
    con: duckdb.DuckDBPyConnection, features: tuple[str, ...], where: str
) -> tuple[Any, Any, Any]:
    import polars as pl

    cols = ", ".join(f"cast({f} as double) as {f}" for f in features)
    table = con.sql(
        f"select stop_id, horizon, actual_delay, {cols} from features where {where}"
    ).to_arrow_table()
    df = pl.DataFrame(table)
    x = df.select(features).to_numpy()
    y = df.get_column("actual_delay").cast(pl.Float64).to_numpy()
    return x, y, df.select("stop_id", "horizon")


def fit(
    con: duckdb.DuckDBPyConnection, name: str, features: tuple[str, ...], valid_from: date
) -> QuantileModel:
    """Fits one model on the temp table ``features``."""
    import lightgbm as lgb
    import numpy as np

    v = f"date '{valid_from.isoformat()}'"
    x_tr, y_tr, _ = _matrix(con, features, f"t < {v}")
    x_va, y_va, _ = _matrix(con, features, f"t >= {v}")
    cats = [features.index(c) for c in CATEGORICAL]
    boosters: dict[float, Any] = {}
    for q in QUANTILES:
        d_tr = lgb.Dataset(x_tr, y_tr, feature_name=list(features), categorical_feature=cats)
        d_va = lgb.Dataset(x_va, y_va, reference=d_tr)
        boosters[q] = lgb.train(
            {**PARAMS, "objective": "quantile", "alpha": q, "metric": "quantile"},
            d_tr,
            num_boost_round=3000,
            valid_sets=[d_va],
            callbacks=[lgb.early_stopping(100, verbose=False)],
        )
    model = QuantileModel(name, features, boosters, 0.0, {})
    med, lo, hi = model.predict(x_va)
    # Conformal widening: the k-th smallest of max(lo - y, y - hi) over n validation rows.
    conformity = np.sort(np.maximum(lo - y_va, y_va - hi))
    k = min(len(conformity) - 1, math.ceil((len(conformity) + 1) * COVERAGE) - 1)
    model.widen = float(conformity[k])
    model.info = {
        "rows_train": len(y_tr),
        "rows_valid": len(y_va),
        "best_iterations": {str(q): b.best_iteration for q, b in boosters.items()},
        "valid_mae": float(np.mean(np.abs(y_va - med))),
        # lo and hi are the raw quantiles here: widen was 0 when they were predicted.
        "valid_raw_coverage": float(np.mean((y_va >= lo) & (y_va <= hi))),
        "widen_min": model.widen,
    }
    return model


def train_without_db(
    stations: list[Station],
    train_files: list[Path],
    change_files: list[Path],
    history_files: list[Path],
    valid_from: date,
) -> QuantileModel:
    con = duckdb.connect()
    archive_inputs(con, train_files, change_files)
    build_features(con, stations, history_files)
    return fit(con, "without_db", FEATURES, valid_from)


def train_with_db(
    stations: list[Station],
    train_files: list[Path],
    change_files: list[Path],
    history_files: list[Path],
    valid_from: date,
) -> QuantileModel:
    con = duckdb.connect()
    archive_db_inputs(con, train_files, change_files)
    build_features(con, stations, history_files, with_db=True)
    return fit(con, "with_db", FEATURES_DB, valid_from)


def collector_predictions(
    models: list[QuantileModel],
    stations: list[Station],
    paths: bl.Paths,
    history_files: list[Path],
) -> duckdb.DuckDBPyConnection:
    """A connection holding ``scored``: one row per snapshot (train running) and model."""
    import polars as pl

    con = duckdb.connect()
    bl.build(con, paths, stations)
    collector_inputs(con)
    build_features(con, stations, history_files, with_db=True)
    frames = []
    for m in models:
        x, _, ids = _matrix(con, m.features, "true")
        med, lo, hi = m.predict(x)
        frames.append(
            ids.with_columns(
                model=pl.lit(m.name), pred=pl.Series(med), lo=pl.Series(lo), hi=pl.Series(hi)
            )
        )
    con.register("preds_arrow", pl.concat(frames).to_arrow())
    con.execute(
        """create or replace temp table scored as
           select s.*, p.model, p.pred, p.lo, p.hi, hour(s.arr_planned) as hour,
                  st.station_name
           from snapshots s join preds_arrow p using (stop_id, horizon)
           join st using (stop_id)
           where s.pers_delay is not null"""
    )
    return con


def scores(con: duckdb.DuckDBPyConnection, by: str = "horizon") -> list[dict[str, Any]]:
    """Per group and model: MAE of the median and of DB and persistence on the same rows,
    and the 80 % range's coverage and width."""
    return query(
        con,
        f"""
        select {by}, model, count(*) as n,
               avg(abs(actual_delay - pers_delay)) as mae_persistence,
               avg(abs(actual_delay - db_delay)) as mae_db,
               avg(abs(actual_delay - pred)) as mae_model,
               avg((actual_delay between lo and hi)::int) as coverage,
               avg(hi - lo) as width
        from scored group by all order by all
        """,
    )


def importance(model: QuantileModel) -> list[tuple[str, float]]:
    """Share of the median model's total gain per feature."""
    b = model.boosters[0.5]
    gains = b.feature_importance("gain")
    total = float(sum(gains)) or 1.0
    return sorted(
        ((n, float(g) / total) for n, g in zip(b.feature_name(), gains, strict=True)),
        key=lambda p: -p[1],
    )


@dataclass
class RunRecord:
    run_id: str
    train_files: list[str]
    valid_from: str
    params: dict[str, Any]
    models: dict[str, dict[str, Any]] = field(default_factory=dict)
    test: list[dict[str, Any]] = field(default_factory=list)


def save_run(runs_dir: Path, models: list[QuantileModel], record: RunRecord) -> Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    for m in models:
        for q, b in m.boosters.items():
            b.save_model(str(runs_dir / f"{record.run_id}.{m.name}.q{int(q * 100)}.lgb.txt"))
    out = runs_dir / f"{record.run_id}.json"
    out.write_text(json.dumps(asdict(record), indent=2, default=str), encoding="utf-8")
    return out


def new_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

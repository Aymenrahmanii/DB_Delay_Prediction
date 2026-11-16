"""Freezing, fingerprints, the one-shot final table and the data bundle."""

import json
import zipfile
from datetime import date
from pathlib import Path

import pytest

from dbdelay.bundle import make_bundle, open_bundle
from dbdelay.final import FinalResult, freeze, load_frozen, markdown, run_files

RUN = "20261116T000000Z"


@pytest.fixture
def runs(tmp_path: Path) -> Path:
    runs = tmp_path / "data" / "runs"
    runs.mkdir(parents=True)
    (runs / f"{RUN}.json").write_text(json.dumps({"models": {"with_db": {}}}))
    for q in (10, 50, 90):
        (runs / f"{RUN}.with_db.q{q}.lgb.txt").write_text(f"tree {q}")
    return runs


def test_freeze_fingerprints_and_test_window(runs: Path, tmp_path: Path) -> None:
    out = tmp_path / "frozen.json"
    frozen = freeze(runs, RUN, out, date(2026, 11, 16))
    assert frozen.test_from == "2026-11-17"  # only days after the freeze count
    assert set(frozen.files) == {f.name for f in run_files(runs, RUN)}
    assert load_frozen(out, runs).run_id == RUN
    with pytest.raises(FileExistsError):
        freeze(runs, RUN, out, date(2026, 11, 20))  # frozen once


def test_a_changed_model_file_is_refused(runs: Path, tmp_path: Path) -> None:
    out = tmp_path / "frozen.json"
    freeze(runs, RUN, out, date(2026, 11, 16))
    (runs / f"{RUN}.with_db.q50.lgb.txt").write_text("retrained")
    with pytest.raises(ValueError, match="fingerprint"):
        load_frozen(out, runs)


def test_markdown_table() -> None:
    row = {
        "model": "with_db",
        "horizon": 60,
        "n": 1234,
        "mae_persistence": 7.5,
        "mae_db": 5.0,
        "mae_model": 4.5,
        "coverage": 0.8,
        "gain_vs_db": 0.1,
        "gain_ci_low": 0.05,
        "gain_ci_high": 0.15,
        "gain_vs_persistence": 0.4,
    }
    result = FinalResult(
        RUN, "2026-11-17", "2026-11-23", ["2026-11-17"], "2026-11-24", False, [row]
    )
    md = markdown(result)
    assert "| 60 min | 1,234 | 7.50 | 5.00 | 4.50 | +10.0% (+5.0%, +15.0%) | +40.0% | 80.0% |" in md
    assert "scored once" in md and "--force" not in md


def test_bundle_round_trip(runs: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)  # no results/final.json here yet
    data = runs.parent
    (data / "tables" / "change_events").mkdir(parents=True)
    (data / "tables" / "plan_stops.parquet").write_bytes(b"pq")
    (data / "tables" / "change_events" / "2026-11-17.parquet").write_bytes(b"pq")
    (data / "polls").mkdir()
    (data / "polls" / "2026-11-17.jsonl").write_text("{}\n")
    (data / "raw").mkdir()
    (data / "raw" / "secret.jsonl.gz").write_bytes(b"raw")
    frozen = tmp_path / "frozen.json"
    freeze(runs, RUN, frozen, date(2026, 11, 16))

    out, _ = make_bundle(data, frozen, run_files(runs, RUN), tmp_path / "b.zip")
    names = zipfile.ZipFile(out).namelist()
    assert "data/tables/change_events/2026-11-17.parquet" in names and "frozen.json" in names
    assert not any("raw" in n for n in names)  # no raw responses in the bundle

    data_dir, frozen_file, expected = open_bundle(out, tmp_path / "unpacked")
    assert expected is None
    assert load_frozen(frozen_file, data_dir / "runs").run_id == RUN


def test_bundle_with_unsafe_paths_is_refused(tmp_path: Path) -> None:
    bad = tmp_path / "bad.zip"
    with zipfile.ZipFile(bad, "w") as z:
        z.writestr("../outside.txt", "x")
    with pytest.raises(ValueError, match="unsafe"):
        open_bundle(bad, tmp_path / "into")

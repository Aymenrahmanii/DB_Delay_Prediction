"""The collected stations: EVA number, name, and whether /rchg is polled every minute."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from importlib import resources
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Station:
    eva: str
    name: str
    busy: bool


def parse_stations(text: str) -> list[Station]:
    stations: list[Station] = []
    seen: set[str] = set()
    for row in csv.DictReader(io.StringIO(text)):
        eva = row["eva"].strip()
        if not eva.isdigit():
            raise ValueError(f"EVA number must be digits: {eva!r}")
        if eva in seen:
            raise ValueError(f"duplicate EVA number: {eva}")
        seen.add(eva)
        stations.append(Station(eva=eva, name=row["name"].strip(), busy=row["busy"].strip() == "1"))
    return stations


def load_stations(path: Path | None = None) -> list[Station]:
    if path is None:
        text = resources.files("dbdelay").joinpath("stations.csv").read_text(encoding="utf-8")
    else:
        text = path.read_text(encoding="utf-8")
    return parse_stations(text)

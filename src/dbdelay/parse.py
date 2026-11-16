"""Parse Timetables XML into stop-level rows.

A *stop* is one train at one station, identified by the API's id
``<ride id>-<yymmddhhmm>-<stop number>``. The trip key (ride id plus the timestamp) is the
same as in the archive, so both sources join on it.

- /plan gives the planned stop: category, number, line, planned times and platforms.
- /fchg and /rchg give *changes* to a stop at the moment of the fetch: changed time
  (``ct``, DB's forecast), changed platform (``cp``), status (``cs``: ``c`` cancelled,
  ``a`` added, ``p`` back to planned) and the cancellation time (``clt``). A change for a
  train that was never planned (an extra train) carries its own ``tl`` and planned times.

Times in the XML are Berlin local time and are kept naive, as in the archive.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache

from lxml import etree

STOP_ID_RE = re.compile(r"^(-?\d+)-(\d{10})-(\d+)$")


@dataclass(frozen=True, slots=True)
class StopId:
    ride_id: str
    trip_key: str
    stop_num: int


def split_stop_id(stop_id: str) -> StopId | None:
    m = STOP_ID_RE.match(stop_id)
    if m is None:
        return None
    ride, ts, num = m.groups()
    return StopId(ride_id=ride, trip_key=f"{ride}-{ts}", stop_num=int(num))


def db_time(value: str | None) -> datetime | None:
    """``yymmddhhmm`` (Berlin local) to a naive datetime."""
    if not value:
        return None
    return _db_time(value)


@lru_cache(maxsize=1 << 16)  # strptime is slow and the same minutes recur millions of times
def _db_time(value: str) -> datetime:
    return datetime.strptime(value, "%y%m%d%H%M")  # noqa: DTZ007 - naive Berlin, as the archive


@dataclass(frozen=True, slots=True)
class PlannedStop:
    stop_id: str
    eva: str
    train_type: str
    train_number: str
    is_long_distance: bool  # tl f="F": DB's own long-distance filter flag
    line: str | None
    arr_planned: datetime | None
    dep_planned: datetime | None
    arr_platform: str | None
    dep_platform: str | None
    origin: str | None  # first station of the arrival path
    destination: str | None  # last station of the departure path


@dataclass(frozen=True, slots=True)
class StopChange:
    """DB's view of one stop at one fetch; ``None`` means the field was not in the change."""

    stop_id: str
    eva: str
    arr_changed: datetime | None
    dep_changed: datetime | None
    arr_platform: str | None
    dep_platform: str | None
    arr_status: str | None
    dep_status: str | None
    arr_cancel_time: datetime | None
    dep_cancel_time: datetime | None
    # Only for stops that are not in the plan (extra trains, added stops).
    train_type: str | None = None
    train_number: str | None = None
    trip_kind: str | None = None  # tl t: "p" planned, "e" extra train
    is_long_distance: bool | None = None
    arr_planned: datetime | None = None
    dep_planned: datetime | None = None

    def state(self) -> tuple[object, ...]:
        """What DB said, without the stop id: two fetches with the same state are one event."""
        return (
            self.arr_changed,
            self.dep_changed,
            self.arr_platform,
            self.dep_platform,
            self.arr_status,
            self.dep_status,
            self.arr_cancel_time,
            self.dep_cancel_time,
        )


def _root(body: bytes) -> etree._Element:
    parser = etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True)
    return etree.fromstring(body, parser)


def _path_end(path: str | None, first: bool) -> str | None:
    if not path:
        return None
    parts = path.split("|")
    return parts[0] if first else parts[-1]


def parse_plan(body: bytes, eva: str) -> list[PlannedStop]:
    stops: list[PlannedStop] = []
    for s in _root(body).iter("s"):
        sid, tl = s.get("id"), s.find("tl")
        if sid is None or tl is None:
            continue
        ar, dp = s.find("ar"), s.find("dp")
        if ar is None and dp is None:
            continue
        stops.append(
            PlannedStop(
                stop_id=sid,
                eva=eva,
                train_type=tl.get("c", ""),
                train_number=tl.get("n", ""),
                is_long_distance=tl.get("f") == "F",
                line=(ar if ar is not None else dp).get("l"),  # type: ignore[union-attr]
                arr_planned=db_time(ar.get("pt")) if ar is not None else None,
                dep_planned=db_time(dp.get("pt")) if dp is not None else None,
                arr_platform=ar.get("pp") if ar is not None else None,
                dep_platform=dp.get("pp") if dp is not None else None,
                origin=_path_end(ar.get("ppth"), first=True) if ar is not None else None,
                destination=_path_end(dp.get("ppth"), first=False) if dp is not None else None,
            )
        )
    return stops


def parse_changes(body: bytes, eva: str) -> list[StopChange]:
    changes: list[StopChange] = []
    for s in _root(body).iter("s"):
        sid = s.get("id")
        if sid is None:
            continue
        ar, dp, tl = s.find("ar"), s.find("dp"), s.find("tl")

        def get(el: etree._Element | None, attr: str) -> str | None:
            return el.get(attr) if el is not None else None

        changes.append(
            StopChange(
                stop_id=sid,
                eva=s.get("eva", eva),
                arr_changed=db_time(get(ar, "ct")),
                dep_changed=db_time(get(dp, "ct")),
                arr_platform=get(ar, "cp"),
                dep_platform=get(dp, "cp"),
                arr_status=get(ar, "cs"),
                dep_status=get(dp, "cs"),
                arr_cancel_time=db_time(get(ar, "clt")),
                dep_cancel_time=db_time(get(dp, "clt")),
                train_type=get(tl, "c"),
                train_number=get(tl, "n"),
                trip_kind=get(tl, "t"),
                is_long_distance=(tl.get("f") == "F") if tl is not None else None,
                arr_planned=db_time(get(ar, "pt")),
                dep_planned=db_time(get(dp, "pt")),
            )
        )
    return changes

from datetime import UTC, datetime

from dbdelay.collector import expected_requests_per_minute, next_grid_time, plan_slots
from dbdelay.config import Settings
from dbdelay.stations import load_stations


def test_next_grid_time_aligns_and_includes_now() -> None:
    now = datetime(2026, 11, 6, 12, 3, 10, tzinfo=UTC)
    assert next_grid_time(now, 300, 0) == datetime(2026, 11, 6, 12, 5, tzinfo=UTC)
    assert next_grid_time(now, 300, 15) == datetime(2026, 11, 6, 12, 5, 15, tzinfo=UTC)
    on_grid = datetime(2026, 11, 6, 12, 5, tzinfo=UTC)
    assert next_grid_time(on_grid, 300, 0) == on_grid


def test_plan_slots_are_berlin_hours() -> None:
    now = datetime(2026, 11, 6, 12, 30, tzinfo=UTC)  # 13:30 in Berlin (CET)
    slots = plan_slots(now, 3)
    assert [s.hour for s in slots] == [13, 14, 15, 16]


def test_plan_slots_across_dst_fall_back() -> None:
    # 2026-10-25: Berlin clocks go from 03:00 CEST back to 02:00 CET.
    now = datetime(2026, 10, 24, 23, 10, tzinfo=UTC)  # 01:10 CEST
    hours = [s.hour for s in plan_slots(now, 3)]
    assert hours == [1, 2, 2, 3]  # the 02 hour happens twice; neither is skipped


def test_bundled_stations_and_budget() -> None:
    stations = load_stations()
    assert len(stations) == 40
    busy = [s for s in stations if s.busy]
    assert len(busy) == 10
    rpm = expected_requests_per_minute(Settings(), len(stations), len(busy))
    assert 15 < rpm < 25  # the plan's budget: about a third of 60/min

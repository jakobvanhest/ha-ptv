"""Trains that drop out of the live feed while still running.

Replays 17 Nov 2026 at East Richmond: the 4:41pm train, running late, was
listed live, then left out of PTV's trip updates while the 4:52 and 5:04
were still listed. The 4:41 vanished from the sensors although it ran.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("homeassistant")
from google.transit import gtfs_realtime_pb2  # noqa: E402

from custom_components.ptv import PTVDataCoordinator, REALTIME_HOLD  # noqa: E402
from custom_components.ptv.const import DEFAULT_MODE  # noqa: E402

ROUTE = "aus:vic:vic-02-GWY:"
NOW = datetime.now(ZoneInfo("Australia/Melbourne"))
NOW_SEC = NOW.hour * 3600 + NOW.minute * 60 + NOW.second
STOP = "19870"

pytestmark = pytest.mark.skipif(NOW.hour >= 22, reason="times would cross midnight")


def dep(trip, sec):
    return {"trip_id": trip, "dep_seconds": NOW_SEC + sec, "direction_id": 0,
            "stop_id": STOP, "stop_sequence": 9, "dep_str": ""}


def make_coordinator():
    c = object.__new__(PTVDataCoordinator)
    c.mode = DEFAULT_MODE
    c.monitored_lines = ["Glen Waverley"]
    c.alert_effects = set()
    c.stations_config = [{"name": "East Richmond", "directions": "outbound", "count": 4}]
    c.station_stop_ids = {"east richmond": {"stop_ids": [STOP]}}
    c.gtfs_cache = {"scheduled_by_station": {"east richmond": [
        dep("t441", 7 * 60),    # due in 7 min, running 6 late
        dep("t452", 18 * 60),
        dep("t504", 30 * 60),
        dep("t529", 55 * 60),
        dep("t554", 80 * 60),
    ]}}
    return c


def feed(*trips):
    """Trip updates: (trip_id, minutes from now, delay min) or (trip_id, "cancelled")."""
    f = gtfs_realtime_pb2.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    for i, (trip, *rest) in enumerate(trips):
        tu = f.entity.add(id=str(i)).trip_update
        tu.trip.trip_id, tu.trip.route_id, tu.trip.direction_id = trip, ROUTE, 0
        if rest == ["cancelled"]:
            tu.trip.schedule_relationship = 3
            continue
        mins, delay = rest
        s = tu.stop_time_update.add(stop_id=STOP, stop_sequence=9)
        s.departure.time = int((NOW + timedelta(minutes=mins)).timestamp())
        s.departure.delay = delay * 60
    return f.SerializeToString()


def poll(c, raw):
    empty = feed()
    data = c._parse_all(raw, empty, empty)
    return data["Glen Waverley"]["stations"]["east richmond"]["outbound"]


def trips(deps):
    return [(d["trip_id"], d["is_realtime"]) for d in deps]


def test_train_missing_from_one_poll_keeps_its_live_time():
    c = make_coordinator()
    first = poll(c, feed(("t441", 13, 6), ("t452", 20, 2), ("t504", 31, 1)))
    second = poll(c, feed(("t452", 20, 2), ("t504", 31, 1)))   # t441 left out
    assert trips(second)[0] == ("t441", True)
    assert second[0]["dt_iso"] == first[0]["dt_iso"]
    assert second[0]["delay_sec"] == 360


def test_missing_train_due_before_live_ones_filled_from_timetable():
    """Even unseen, a timetabled train before the last live one is listed."""
    c = make_coordinator()
    deps = poll(c, feed(("t452", 20, 2), ("t504", 31, 1)))
    assert trips(deps) == [("t441", False), ("t452", True), ("t504", True), ("t529", False)]


def test_held_train_dropped_when_cancelled():
    c = make_coordinator()
    poll(c, feed(("t441", 13, 6), ("t452", 20, 2)))
    deps = poll(c, feed(("t441", "cancelled"), ("t452", 20, 2)))
    assert "t441" not in {d["trip_id"] for d in deps}


def test_held_train_dropped_once_its_time_passes():
    c = make_coordinator()
    poll(c, feed(("t441", 13, 6), ("t452", 20, 2)))
    (key, memory), = ((k, m) for k, m in c._rt_seen.items() if m)
    hit, seen_at = memory["t441"]
    memory["t441"] = ({**hit, "actual": hit["actual"] - timedelta(minutes=14)}, seen_at)
    deps = poll(c, feed(("t452", 20, 2)))
    # Gone from the live list; the timetable doesn't bring it back either,
    # since its scheduled time is still ahead only in this test's clock.
    assert ("t441", True) not in trips(deps)


def test_held_train_dropped_after_hold_window():
    c = make_coordinator()
    poll(c, feed(("t441", 13, 6), ("t452", 20, 2)))
    for memory in c._rt_seen.values():
        for trip, (hit, seen_at) in list(memory.items()):
            memory[trip] = (hit, seen_at - REALTIME_HOLD - timedelta(seconds=1))
    deps = poll(c, feed(("t452", 20, 2)))
    assert ("t441", True) not in trips(deps)


def test_departed_train_not_filled_back_from_timetable():
    """A train that left early isn't shown again at its timetabled time."""
    c = make_coordinator()
    deps = poll(c, feed(("t441", -1, 0), ("t452", 20, 2)))
    assert "t441" not in {d["trip_id"] for d in deps}

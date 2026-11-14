"""Realtime trip updates matched to station departures.

Covers updates as they may arrive from the PTV feed: no direction_id, a
delay with no absolute time, a stop_id for a different platform than the
timetable's, a skipped stop, and a cancelled trip.
"""
from datetime import datetime, timedelta

import pytest

pytest.importorskip("homeassistant")
from google.transit import gtfs_realtime_pb2  # noqa: E402

from custom_components.ptv import PTVDataCoordinator  # noqa: E402
from custom_components.ptv.const import DEFAULT_MODE  # noqa: E402

ROUTE = "aus:vic:vic-02-HBE:"
NOW = datetime.now().astimezone()
NOW_SEC = NOW.hour * 3600 + NOW.minute * 60 + NOW.second

pytestmark = pytest.mark.skipif(NOW.hour >= 22, reason="times would cross midnight")


def dep(trip, sec, direction, stop="11001", seq=5):
    return {"trip_id": trip, "dep_seconds": NOW_SEC + sec, "direction_id": direction,
            "stop_id": stop, "stop_sequence": seq, "dep_str": ""}


def make_coordinator():
    c = object.__new__(PTVDataCoordinator)
    c.mode = DEFAULT_MODE
    c.monitored_lines = ["Hurstbridge"]
    c.alert_effects = set()
    c.stations_config = [{"name": "Ivanhoe", "directions": "citybound", "count": 4}]
    c.station_stop_ids = {"ivanhoe": {"stop_ids": ["11001", "11002"]}}
    c.gtfs_cache = {"scheduled_by_station": {"ivanhoe": [
        dep("delay_only", 300, 1),
        dep("other_platform", 600, 1, seq=7),
        dep("skips", 900, 1),
        dep("cancelled", 1200, 1),
        dep("plain", 1500, 1),
        dep("static_only", 1800, 1),
        dep("static_only_2", 2100, 1),
    ]}}
    return c


def feed(*updates):
    f = gtfs_realtime_pb2.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    for i, (trip, build) in enumerate(updates):
        e = f.entity.add(id=str(i))
        e.trip_update.trip.trip_id = trip
        e.trip_update.trip.route_id = ROUTE
        build(e.trip_update)
    return f.SerializeToString()


def delay_only(tu):                      # no direction_id, delay but no time
    s = tu.stop_time_update.add(stop_id="11002", stop_sequence=5)
    s.departure.delay = 120


def other_platform(tu):                  # stop_id not in the station's list
    tu.trip.direction_id = 1
    s = tu.stop_time_update.add(stop_id="99999", stop_sequence=7)
    s.departure.time = int((NOW + timedelta(seconds=660)).timestamp())


def skips(tu):
    tu.trip.direction_id = 1
    tu.stop_time_update.add(stop_id="11001", stop_sequence=5, schedule_relationship=1)


def cancelled(tu):
    tu.trip.direction_id = 1
    tu.trip.schedule_relationship = 3


def plain(tu):
    tu.trip.direction_id = 1
    s = tu.stop_time_update.add(stop_id="11001", stop_sequence=5)
    s.departure.time = int((NOW + timedelta(seconds=1500)).timestamp())


def departures():
    c = make_coordinator()
    raw = feed(("delay_only", delay_only), ("other_platform", other_platform),
               ("skips", skips), ("cancelled", cancelled), ("plain", plain))
    empty = feed()
    data = c._parse_all(raw, empty, empty)
    return data["Hurstbridge"]["stations"]["ivanhoe"]["citybound"]


def test_live_departures_matched_and_ordered():
    d = departures()
    assert [e["trip_id"] for e in d] == ["delay_only", "other_platform", "plain", "static_only"]
    assert [e["is_realtime"] for e in d] == [True, True, True, False]


def test_delay_only_update_uses_scheduled_time_plus_delay():
    first = departures()[0]
    when = datetime.fromisoformat(first["dt_iso"])
    expected = NOW.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=NOW_SEC + 420)
    assert abs((when - expected).total_seconds()) < 1
    assert first["delay_sec"] == 120


def test_cancelled_and_skipping_trains_not_filled_from_timetable():
    trips = {e["trip_id"] for e in departures()}
    assert "cancelled" not in trips and "skips" not in trips

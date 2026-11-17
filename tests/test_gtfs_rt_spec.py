"""Stop time updates read the way the GTFS-Realtime spec defines them.

https://gtfs.org/documentation/realtime/reference/#message-stoptimeupdate
- An update applies to the stops after it, up to the next update, so a
  delay at an earlier stop carries on to a station the feed doesn't list.
- It never applies backwards to stops before it.
- NO_DATA means no live information (from that stop on, until the next
  update): the timetable's time is shown.
- SKIPPED applies to that stop only; a delay carries through it.
- A time with no delay is compared with the timetable for the delay.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("homeassistant")
from google.transit import gtfs_realtime_pb2  # noqa: E402

from custom_components.ptv import PTVDataCoordinator  # noqa: E402
from custom_components.ptv.const import DEFAULT_MODE  # noqa: E402

ROUTE = "aus:vic:vic-02-GWY:"
NOW = datetime.now(ZoneInfo("Australia/Melbourne"))
NOW_SEC = NOW.hour * 3600 + NOW.minute * 60 + NOW.second
STATION_STOP, STATION_SEQ = "19870", 9
DUE = 10 * 60          # the train is timetabled at the station in 10 min

pytestmark = pytest.mark.skipif(NOW.hour >= 22, reason="times would cross midnight")


def make_coordinator():
    c = object.__new__(PTVDataCoordinator)
    c.mode = DEFAULT_MODE
    c.monitored_lines = ["Glen Waverley"]
    c.alert_effects = set()
    c.stations_config = [{"name": "East Richmond", "directions": "outbound", "count": 2}]
    c.station_stop_ids = {"east richmond": {"stop_ids": [STATION_STOP]}}
    c.gtfs_cache = {"scheduled_by_station": {"east richmond": [
        {"trip_id": "t1", "dep_seconds": NOW_SEC + DUE, "direction_id": 0,
         "stop_id": STATION_STOP, "stop_sequence": STATION_SEQ, "dep_str": ""},
        {"trip_id": "t2", "dep_seconds": NOW_SEC + DUE + 1800, "direction_id": 0,
         "stop_id": STATION_STOP, "stop_sequence": STATION_SEQ, "dep_str": ""},
    ]}}
    return c


def first(*stops):
    """Station's first departure for trip t1 with these stop time updates.

    Each stop: dict(seq, stop, delay, time (minutes from now), rel).
    """
    f = gtfs_realtime_pb2.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    tu = f.entity.add(id="1").trip_update
    tu.trip.trip_id, tu.trip.route_id, tu.trip.direction_id = "t1", ROUTE, 0
    for st in stops:
        u = tu.stop_time_update.add(stop_sequence=st["seq"], stop_id=st.get("stop", "x"))
        if "rel" in st:
            u.schedule_relationship = st["rel"]
        if "delay" in st:
            u.departure.delay = st["delay"] * 60
        if "time" in st:
            u.departure.time = int((NOW + timedelta(minutes=st["time"])).timestamp())
    raw = f.SerializeToString()
    empty = gtfs_realtime_pb2.FeedMessage()
    empty.header.gtfs_realtime_version = "2.0"
    data = make_coordinator()._parse_all(raw, empty.SerializeToString(),
                                         empty.SerializeToString())
    deps = data["Glen Waverley"]["stations"]["east richmond"]["outbound"]
    return next(d for d in deps if d["trip_id"] == "t1")


def minutes_from_now(dep):
    return round((datetime.fromisoformat(dep["dt_iso"]) - NOW).total_seconds() / 60)


def test_delay_at_earlier_stop_carries_on_to_unlisted_station():
    dep = first({"seq": 6, "delay": 7})          # Richmond only, 7 min late
    assert dep["is_realtime"] and dep["delay_sec"] == 420
    assert minutes_from_now(dep) == 17


def test_nearest_earlier_update_decides():
    dep = first({"seq": 4, "delay": 2}, {"seq": 7, "delay": 9})
    assert dep["delay_sec"] == 540


def test_update_after_the_station_does_not_apply_backwards():
    dep = first({"seq": 12, "delay": 9})
    assert not dep["is_realtime"] and minutes_from_now(dep) == 10


def test_no_data_at_earlier_stop_shows_timetable_time():
    dep = first({"seq": 4, "delay": 5}, {"seq": 7, "rel": 2})
    assert not dep["is_realtime"] and minutes_from_now(dep) == 10


def test_no_data_at_station_shows_timetable_time():
    dep = first({"seq": STATION_SEQ, "stop": STATION_STOP, "rel": 2})
    assert not dep["is_realtime"] and minutes_from_now(dep) == 10


def test_delay_carries_through_a_skipped_stop():
    dep = first({"seq": 5, "delay": 4}, {"seq": 7, "rel": 1})
    assert dep["is_realtime"] and dep["delay_sec"] == 240


def test_time_without_delay_gets_delay_from_timetable():
    dep = first({"seq": STATION_SEQ, "stop": STATION_STOP, "time": 16})
    assert dep["is_realtime"] and dep["delay_sec"] == 360


def test_listed_station_with_no_event_carries_earlier_delay():
    dep = first({"seq": 6, "delay": 3}, {"seq": STATION_SEQ, "stop": STATION_STOP})
    assert dep["is_realtime"] and dep["delay_sec"] == 180

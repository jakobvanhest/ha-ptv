"""Coordinator behaviour: rate-limit backoff and times around midnight."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("homeassistant")
from homeassistant.util import dt as dt_util  # noqa: E402
import aiohttp  # noqa: E402

import custom_components.ptv as ptv  # noqa: E402
from custom_components.ptv import PTVDataCoordinator  # noqa: E402
from custom_components.ptv.const import DEFAULT_MODE  # noqa: E402


def bare(**attrs):
    c = object.__new__(PTVDataCoordinator)
    c.mode = DEFAULT_MODE
    c.api_base = "https://api.example/metro"
    c.api_key = "k"
    c._last_feeds = {}
    c._stale_feeds = set()
    c._rate_limited_until = {}
    c.gtfs_cache = {}
    for k, v in attrs.items():
        setattr(c, k, v)
    return c


class Resp:
    """Stand-in for an aiohttp response."""

    def __init__(self, status, body=b"feed", headers=None):
        self.status, self.body, self.headers = status, body, headers or {}

    async def read(self):
        return self.body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class Session:
    def __init__(self, answers):
        self.answers, self.calls = iter(answers), 0

    def get(self, *a, **k):
        self.calls += 1
        answer = next(self.answers)
        if isinstance(answer, Exception):
            raise answer
        return answer


def fetch(c, path="/trip-updates"):
    # A private loop: asyncio.run would clear the thread's loop, which other
    # plugins' fixtures may rely on.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(c._async_fetch_feed(path))
    finally:
        loop.close()


def test_429_pauses_the_endpoint_and_serves_last_copy():
    session = Session([Resp(200, b"good"), Resp(429, headers={"Retry-After": "120"})])
    c = bare(_session=session)
    assert fetch(c) == b"good"
    assert fetch(c) == b"good"                         # 429: last copy
    until = c._rate_limited_until["/trip-updates"]
    assert timedelta(seconds=110) < until - datetime.now(until.tzinfo) <= timedelta(seconds=120)
    # Further polls during the pause don't call PTV at all.
    assert fetch(c) == b"good"
    assert session.calls == 2


def test_network_error_uses_last_copy_then_fails_when_stale():
    session = Session([Resp(200, b"good"), aiohttp.ClientConnectionError("reset"),
                       aiohttp.ClientConnectionError("reset")])
    c = bare(_session=session)
    assert fetch(c) == b"good"
    assert fetch(c) == b"good"
    path = "/trip-updates"
    c._last_feeds[path] = (b"good", datetime.now(timezone.utc) - timedelta(hours=1))
    with pytest.raises(ptv.FeedError):
        fetch(c)


def test_429_without_retry_after_uses_default_backoff():
    c = bare()
    assert c._retry_after(Resp(429)) == ptv.RATE_LIMIT_BACKOFF
    assert c._retry_after(Resp(429, headers={"Retry-After": "99999"})) == timedelta(seconds=900)


def test_fallback_after_midnight_does_not_wrap_to_the_morning():
    """A last live train at 00:10 must not pull in the day's 05:00 train."""
    today = dt_util.now()
    day = today.strftime("%Y%m%d")
    c = bare(
        monitored_lines=["Hurstbridge"],
        stations_config=[{"name": "Ivanhoe", "directions": "citybound", "count": 2}],
        gtfs_cache={"cached_date": day, "scheduled_by_station": {"ivanhoe": [
            {"trip_id": "morning", "dep_seconds": 5 * 3600, "direction_id": 1},
            {"trip_id": "late", "dep_seconds": 24 * 3600 + 20 * 60, "direction_id": 1},
        ]}},
    )
    midnight = c._cache_midnight()
    results = {"Hurstbridge": {"stations": {"ivanhoe": {"citybound": [{
        "dt_iso": (midnight + timedelta(hours=24, minutes=10)).isoformat(),
        "delay_sec": 0, "is_realtime": True, "is_express": False, "trip_id": "live",
    }], "outbound": []}}}}
    c._cancelled_trips, c._skipped_at = set(), {"ivanhoe": set()}
    c._apply_static_fallback(results)
    added = results["Hurstbridge"]["stations"]["ivanhoe"]["citybound"][1:]
    assert [e["trip_id"] for e in added] == ["late"]
    assert datetime.fromisoformat(added[0]["dt_iso"]) == midnight + timedelta(hours=24, minutes=20)


def test_times_shown_in_home_assistant_time_zone():
    """Formatted times follow Home Assistant's zone, not the server clock's."""
    from custom_components.ptv.sensor import _fmt
    assert _fmt("2026-11-14T22:42:00+00:00", 0, True) == "9:42 AM (on time)"


def test_tram_binary_sensors_not_called_lines():
    import types
    from custom_components.ptv.binary_sensor import PTVDelayBinarySensor
    tram = PTVDelayBinarySensor(types.SimpleNamespace(mode="tram"), "Route 58")
    train = PTVDelayBinarySensor(types.SimpleNamespace(mode="train"), "Hurstbridge")
    assert tram.name == "PTV Route 58 Delays"
    assert train.name == "PTV Hurstbridge Line Delays"     # unchanged for trains

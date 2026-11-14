"""Coordinator behaviour: rate-limit backoff and times around midnight."""
from datetime import datetime, timedelta

import pytest

pytest.importorskip("homeassistant")
import requests  # noqa: E402

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
    def __init__(self, status, body=b"feed", headers=None):
        self.status_code, self.content, self.headers = status, body, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def test_429_pauses_the_endpoint_and_serves_last_copy(monkeypatch):
    calls = []
    answers = iter([Resp(200, b"good"), Resp(429, headers={"Retry-After": "120"})])
    monkeypatch.setattr(ptv.requests, "get", lambda *a, **k: calls.append(1) or next(answers))
    c = bare()
    assert c._fetch_feed("/trip-updates") == b"good"
    assert c._fetch_feed("/trip-updates") == b"good"      # 429: last copy
    until = c._rate_limited_until["/trip-updates"]
    assert timedelta(seconds=110) < until - datetime.now(until.tzinfo) <= timedelta(seconds=120)
    # Further polls during the pause don't call PTV at all.
    assert c._fetch_feed("/trip-updates") == b"good"
    assert len(calls) == 2


def test_429_without_retry_after_uses_default_backoff():
    c = bare()
    assert c._retry_after(Resp(429)) == ptv.RATE_LIMIT_BACKOFF
    assert c._retry_after(Resp(429, headers={"Retry-After": "99999"})) == timedelta(seconds=900)


def test_fallback_after_midnight_does_not_wrap_to_the_morning():
    """A last live train at 00:10 must not pull in the day's 05:00 train."""
    today = datetime.now().astimezone()
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

"""Timetable auto-update scenarios against a fake portal and release.

Loads gtfs_static without Home Assistant, so these run with just pytest
and requests installed.
"""
import io
import json
import sys
import types
import zipfile
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1] / "custom_components" / "ptv"
pkg = types.ModuleType("ptv")
pkg.__path__ = [str(REPO)]
sys.modules["ptv"] = pkg
import ptv.gtfs_static as gs  # noqa: E402
import ptv.const as const  # noqa: E402

TODAY = datetime.now(ZoneInfo("Australia/Melbourne"))
DOW = TODAY.strftime("%A").lower()
DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
OTHER = DAYS[(DAYS.index(DOW) + 1) % 7]
YDAY = DAYS[(DAYS.index(DOW) - 1) % 7]


def _csv(rows):
    return "\n".join(",".join(r) for r in rows) + "\n"


def inner_zip(tag: str, route: str) -> bytes:
    """Today's service runs at 08:00+tag; another weekday's at 07:00."""
    start = (TODAY - timedelta(days=30)).strftime("%Y%m%d")
    end = (TODAY + timedelta(days=30)).strftime("%Y%m%d")
    head = ["service_id", "start_date", "end_date"] + DAYS
    cal = [head,
           ["TODAY", start, end] + ["1" if d == DOW else "0" for d in DAYS],
           ["OTHER", start, end] + ["1" if d == OTHER else "0" for d in DAYS],
           ["YDAY", start, end] + ["1" if d == YDAY else "0" for d in DAYS]]
    files = {
        "stops.txt": _csv([["stop_id", "stop_name"],
                           ["100", "Ivanhoe Railway Station"],
                           ["101", "Ivanhoe Railway Station"]]),
        "routes.txt": _csv([["route_id", "route_short_name"],
                            [route, "x"], ["aus:vic:vic-02-ALM:", "a"],
                            ["aus:vic:vic-02-BEG:", "b"]]),
        "trips.txt": _csv([["route_id", "service_id", "trip_id", "direction_id", "trip_headsign", "block_id"],
                           [route, "TODAY", "t1", "1", "City", ""],
                           [route, "OTHER", "t2", "1", "City", ""],
                           [route, "YDAY", "late_yday", "1", "City", ""],
                           [route, "TODAY", "late_today", "1", "City", ""]]),
        "stop_times.txt": _csv([["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
                                ["t1", f"08:0{tag}:00", f"08:0{tag}:00", "100", "1"],
                                ["t2", "07:00:00", "07:00:00", "100", "1"],
                                # Yesterday's service, after midnight: today 00:30
                                ["late_yday", "24:30:00", "24:30:00", "100", "1"],
                                # Yesterday's service before midnight: not today
                                ["late_yday", "23:00:00", "23:00:00", "101", "2"],
                                # Today's service after midnight: tomorrow 01:00
                                ["late_today", "25:00:00", "25:00:00", "100", "1"]]),
        "calendar.txt": _csv(cal),
        "calendar_dates.txt": _csv([["service_id", "date", "exception_type"]]),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for k, v in files.items():
            z.writestr(k, v)
    return buf.getvalue()


def release_zip(tag: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("2/google_transit.zip", inner_zip(tag, "aus:vic:vic-02-HBE:"))
        z.writestr("3/google_transit.zip", inner_zip(tag, "aus:vic:vic-03-109:"))
    return buf.getvalue()


class FakeResp:
    def __init__(self, status=200, body=b"", js=None):
        self.status_code, self.content, self._js = status, body, js

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code} Client Error")

    def json(self):
        return self._js

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Portal:
    def __init__(self):
        self.api_up = True
        self.version = "a"
        self.files = {"https://x/res-a/gtfs.zip": release_zip("1")}
        self.calls = []

    def publish(self, tag, url):
        self.version = tag
        self.files[url] = release_zip(tag)
        self.url = url

    url = "https://x/res-a/gtfs.zip"

    def get(self, url, **kw):
        self.calls.append(url)
        if url == const.GTFS_PACKAGE_API:
            if not self.api_up:
                import requests
                raise requests.ConnectionError("portal down")
            return FakeResp(js={"success": True, "result": {"resources": [
                {"id": "res", "format": "ZIP", "state": "active", "url": self.url,
                 "last_modified": f"2026-11-1{len(self.version)}T00:00:{self.version}", "size": 1}]}})
        if url in self.files:
            return FakeResp(body=self.files[url])
        return FakeResp(status=404)

    def downloads(self):
        return [c for c in self.calls if c != const.GTFS_PACKAGE_API]


@pytest.fixture
def portal(monkeypatch):
    p = Portal()
    monkeypatch.setattr(gs.requests, "get", p.get)
    return p


ROUTES = {"aus:vic:vic-02-HBE:"}


def run(tmp_path, stations=("Ivanhoe",)):
    return gs.download_and_cache_gtfs(str(tmp_path), list(stations), ROUTES, "train")


def deps(cache):
    """Today's daytime departures (after-midnight ones are tested apart)."""
    return [d["dep_str"] for d in cache["scheduled_by_station"]["ivanhoe"]
            if d["trip_id"] not in ("late_yday", "late_today")]


def test_first_run_downloads_and_uses_todays_services(tmp_path, portal):
    c = run(tmp_path)
    assert deps(c) == ["08:01:00"]            # not the other weekday's 07:00
    assert c["release_check_ok"] and c["has_service_today"]
    assert (tmp_path / "ptv_gtfs" / "train.zip").exists()
    assert (tmp_path / "ptv_gtfs" / "tram.zip").exists()   # shared download
    assert not list((tmp_path / "ptv_gtfs").glob("*.tmp"))
    assert len(portal.downloads()) == 1


def test_same_day_uses_cache_without_network(tmp_path, portal):
    run(tmp_path)
    portal.calls.clear()
    run(tmp_path)
    assert portal.calls == []


def test_new_day_unchanged_release_skips_download(tmp_path, portal):
    run(tmp_path)
    _age_cache(tmp_path)
    portal.calls.clear()
    c = run(tmp_path)
    assert portal.calls == [const.GTFS_PACKAGE_API]
    assert c["cached_date"] == TODAY.strftime("%Y%m%d")


def test_new_release_new_link_is_downloaded(tmp_path, portal):
    run(tmp_path)
    _age_cache(tmp_path)
    portal.publish("2", "https://x/res-b/gtfs.zip")
    c = run(tmp_path)
    assert deps(c) == ["08:02:00"]
    assert c["release"]["url"] == "https://x/res-b/gtfs.zip"


def test_tram_entry_reuses_train_download(tmp_path, portal):
    run(tmp_path)
    portal.calls.clear()
    gs.download_and_cache_gtfs(str(tmp_path), [], {"aus:vic:vic-03-109:"}, "tram")
    assert portal.downloads() == []


def test_portal_down_keeps_timetable_on_disk(tmp_path, portal):
    run(tmp_path)
    _age_cache(tmp_path)
    portal.api_up = False
    c = run(tmp_path)
    assert deps(c) == ["08:01:00"]
    assert c["release_check_ok"] is False
    assert c["release_checked_at"]           # carried from the last good check
    assert "portal down" in c["timetable_error"]


def test_download_404_keeps_timetable_on_disk(tmp_path, portal):
    run(tmp_path)
    _age_cache(tmp_path)
    portal.publish("3", "https://x/res-c/gtfs.zip")
    del portal.files["https://x/res-c/gtfs.zip"]
    c = run(tmp_path)
    assert deps(c) == ["08:01:00"]
    assert "404" in c["timetable_error"]
    assert (tmp_path / "ptv_gtfs" / "train.zip").exists()


def test_legacy_cache_never_shows_another_days_times(tmp_path, portal):
    """Today's bug: only an old JSON cache, and the download 404s."""
    legacy = {"cached_at": "2026-11-02T00:00:00", "cached_date": "20261102", "schema": 2,
              "stops": {"ivanhoe": {"stop_name": "Ivanhoe", "stop_ids": ["100"]}},
              "scheduled_by_station": {"ivanhoe": [{"dep_seconds": 1, "direction_id": 1}]},
              "through_trips": {"x": "y"}}
    (tmp_path / "ptv_gtfs_cache.json").write_text(json.dumps(legacy))
    portal.api_up = False
    portal.files.clear()
    c = run(tmp_path)
    assert c["scheduled_by_station"] == {"ivanhoe": []}
    assert c["stops"]["ivanhoe"]["stop_ids"] == ["100"]   # stations still resolve
    assert c["cached_date"] == "20261102"                  # coordinator retries
    assert const.GTFS_FALLBACK_URL in portal.calls         # last-known link tried


def test_legacy_cache_upgrades_when_portal_works(tmp_path, portal):
    (tmp_path / "ptv_gtfs_cache.json").write_text(json.dumps(
        {"cached_date": "20261102", "schema": 2, "stops": {}, "scheduled_by_station": {}}))
    c = run(tmp_path)
    assert deps(c) == ["08:01:00"] and c["schema"] == const.GTFS_CACHE_SCHEMA


def test_nothing_available_raises(tmp_path, portal):
    portal.api_up = False
    portal.files.clear()
    with pytest.raises(RuntimeError):
        run(tmp_path)


def test_resolve_picks_newest_zip(monkeypatch):
    js = {"success": True, "result": {"resources": [
        {"id": "old", "format": "ZIP", "url": "https://o/gtfs.zip", "last_modified": "2026-10-01"},
        {"id": "pdf", "format": "PDF", "url": "https://o/notes.pdf", "last_modified": "2026-12-01"},
        {"id": "new", "format": "ZIP", "url": "https://n/gtfs.zip", "last_modified": "2026-11-13"},
    ]}}
    monkeypatch.setattr(gs.requests, "get", lambda *a, **k: FakeResp(js=js))
    r = gs.resolve_gtfs_release()
    assert r["url"] == "https://n/gtfs.zip" and r["resource_id"] == "new"


def _age_cache(tmp_path):
    p = tmp_path / "ptv_gtfs_cache.json"
    c = json.loads(p.read_text())
    c["cached_date"] = "20000101"
    c["release_checked_date"] = "20000101"
    p.write_text(json.dumps(c))


def test_after_midnight_trains_kept(tmp_path, portal):
    c = run(tmp_path)
    late = {d["trip_id"]: d for d in c["scheduled_by_station"]["ivanhoe"]
            if d["trip_id"].startswith("late")}
    # Yesterday's 24:30 is today's 00:30; its 23:00 stop belongs to yesterday.
    assert late["late_yday"]["dep_seconds"] == 1800 and late["late_yday"]["previous_day"]
    assert late["late_today"]["dep_seconds"] == 25 * 3600
    assert len([d for d in c["scheduled_by_station"]["ivanhoe"]
                if d["trip_id"] == "late_yday"]) == 1


def test_disk_only_makes_no_network_calls(tmp_path, portal):
    run(tmp_path)                      # puts a timetable on disk
    _age_cache(tmp_path)
    portal.calls.clear()
    c = gs.download_and_cache_gtfs(str(tmp_path), ["Ivanhoe"], ROUTES, "train", False)
    assert portal.calls == []
    assert deps(c) == ["08:01:00"]
    assert c["release_checked_date"] != c["cached_date"]   # still to check today


def test_disk_only_with_nothing_on_disk_raises(tmp_path, portal):
    with pytest.raises(RuntimeError):
        gs.download_and_cache_gtfs(str(tmp_path), ["Ivanhoe"], ROUTES, "train", False)
    assert portal.calls == []


def test_checked_today_is_not_asked_again(tmp_path, portal):
    run(tmp_path)
    portal.api_up = False              # check fails, but it was attempted today
    _age_cache(tmp_path)
    run(tmp_path)
    portal.calls.clear()
    run(tmp_path)
    assert portal.calls == []

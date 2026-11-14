"""GTFS Static schedule loader for PTV."""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import shutil
import threading
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from .const import (
    GTFS_API_TIMEOUT,
    GTFS_CACHE_FILE,
    GTFS_CACHE_SCHEMA,
    GTFS_DOWNLOAD_TIMEOUT,
    GTFS_FALLBACK_URL,
    GTFS_PACKAGE_API,
    GTFS_RELEASE_FILE,
    GTFS_STORE_DIR,
    GTFS_USER_AGENT,
    MODES,
    MODE_TRAIN,
    MODE_TRAM,
    TUNNEL_SIDES,
    mode_info,
    through_running_routes,
)

_LOGGER = logging.getLogger(__name__)

try:  # Home Assistant's configured time zone when running inside it
    from homeassistant.util import dt as _dt_util

    def _local_now() -> datetime:
        return _dt_util.now()
except ImportError:  # standalone (tests, tools)
    def _local_now() -> datetime:
        return datetime.now().astimezone()

# Train and tram entries share one download; serialise them so the second
# sees the first's result instead of downloading the same release again.
_DOWNLOAD_LOCK = threading.Lock()

_METRO_CODES = {
    "ALM","BEG","CBE","CGB","FKN","GWY","HBE",
    "LIL","MDD","PKM","SHM","STY","SUY","UFD","WER","WIL",
}


def _find_mode_entry(outer: zipfile.ZipFile, mode: str = MODE_TRAIN) -> str:
    """Name of the inner GTFS zip for a transport mode within the release."""
    info = mode_info(mode)
    known = info["gtfs_inner_zip"]
    # Route IDs are "aus:vic:vic-02-..." for train, "vic-03-" for tram
    marker = f"vic-{known.split('/')[0].zfill(2)}-"

    if known in outer.namelist():
        return known
    _LOGGER.warning("Known %s zip path %s missing, scanning...", mode, known)
    for entry in sorted(outer.namelist()):
        if not entry.endswith("google_transit.zip"):
            continue
        try:
            with outer.open(entry) as raw:
                inner_z = zipfile.ZipFile(io.BytesIO(raw.read()))
            if "routes.txt" not in inner_z.namelist():
                continue
            with inner_z.open("routes.txt") as f:
                content = f.read().decode("utf-8-sig")
            if mode == MODE_TRAIN:
                if len([c for c in _METRO_CODES if f"-{c}:" in content]) >= 3:
                    return entry
            elif marker in content:
                return entry
        except Exception:
            pass
    raise RuntimeError(f"Could not locate {mode} data inside GTFS zip.")


def _clean_stop_name(name: str, mode: str) -> str:
    """Strip mode-specific noise from a stop name.

    Train stops are "Ivanhoe Railway Station"; tram stops are
    "Casino/Southbank/Queens Bridge St #115", where the number identifies
    the stop and must be kept.
    """
    if mode == MODE_TRAM:
        return name.strip()
    return (
        name
        .replace(" Railway Station","")
        .replace(" Train Station","")
        .replace(" Station","")
        .split("(")[0].strip()
    )


def _load_stops(z: zipfile.ZipFile, mode: str = MODE_TRAIN) -> dict[str, dict]:
    """Group stops by name. Each direction is its own stop_id under one name."""
    grouped: dict[str, dict] = {}
    with z.open("stops.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            sid = row.get("stop_id", "").strip()
            name = row.get("stop_name", "").strip()
            if not sid or not name:
                continue
            if mode == MODE_TRAIN and ("Bus" in name or "Rail Replacement" in name):
                continue
            if not sid.replace("-","").replace("_","").isdigit():
                continue
            clean = _clean_stop_name(name, mode)
            # A few rows carry the platform number as the stop name instead of
            # the station's (e.g. Berwick stops 27882/27883, named "1" and "2").
            # They never appear in stop_times, so drop them.
            if not clean or clean.isdigit():
                continue
            key = clean.lower()
            if key not in grouped:
                grouped[key] = {"stop_name": clean, "stop_ids": [sid]}
            elif sid not in grouped[key]["stop_ids"]:
                grouped[key]["stop_ids"].append(sid)
    return grouped


def _load_direction_labels(z: zipfile.ZipFile, route_ids: set[str]) -> dict[str, dict]:
    """Map route_id -> {"0": headsign, "1": headsign}.

    Trams have no citybound/outbound; the destination is the useful label.
    Short workings produce minority headsigns, so take the most common one.
    """
    counts: dict[str, dict[str, Counter]] = {}
    with z.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            rid = row.get("route_id", "")
            if not any(rid.startswith(r) for r in route_ids):
                continue
            head = (row.get("trip_headsign") or "").strip()
            if not head:
                continue
            counts.setdefault(rid, {}).setdefault(row.get("direction_id", "0"), Counter())[head] += 1
    return {
        rid: {d: c.most_common(1)[0][0] for d, c in dirs.items()}
        for rid, dirs in counts.items()
    }


def _active_service_ids(z: zipfile.ZipFile, today_str: str, dow: str) -> set[str]:
    active: set[str] = set()
    with z.open("calendar.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            if (row.get(dow) == "1"
                    and row.get("start_date","") <= today_str <= row.get("end_date","")):
                active.add(row["service_id"])
    with z.open("calendar_dates.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            if row.get("date") == today_str:
                if row.get("exception_type") == "1":
                    active.add(row["service_id"])
                elif row.get("exception_type") == "2":
                    active.discard(row["service_id"])
    return active


def _through_trips(z: zipfile.ZipFile, route_ids: set[str], active_sids: set[str]) -> dict[str, str]:
    """Trips that continue the selected lines' trains through the Metro Tunnel.

    Maps each such trip_id to the selected route it continues as. The two
    halves of a train share a block_id, and every tunnel block pairs Sunbury
    with exactly one of Pakenham or Cranbourne, so a Pakenham rider gets the
    Sunbury trips that become Pakenham trains — not Cranbourne ones.
    """
    through = tuple(through_running_routes(route_ids))
    if not through:
        return {}
    own = tuple(r for r in route_ids if r in TUNNEL_SIDES)
    own_blocks: dict[tuple, str] = {}
    candidates: list[tuple[str, tuple]] = []
    with z.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            block = row.get("block_id")
            if not block or row.get("service_id") not in active_sids:
                continue
            key = (row["service_id"], block)
            rid = row.get("route_id", "")
            for r in own:
                if rid.startswith(r):
                    own_blocks[key] = r
            if rid.startswith(through):
                candidates.append((row["trip_id"], key))
    return {tid: own_blocks[key] for tid, key in candidates if key in own_blocks}


DAY_SECONDS = 86400


def _load_scheduled_departures(
    metro_z: zipfile.ZipFile,
    stop_ids: set[str],
    route_ids: set[str],
    today_str: str,
    dow: str,
    through_trips: dict[str, str] | None = None,
    previous_day: tuple[str, str, dict[str, str]] | None = None,
) -> list[dict]:
    """Return all departures at stop_ids today, sorted by time.

    through_trips (from _through_trips) adds the tunnel continuations of the
    selected lines' trains, with direction flipped to read relative to them.

    previous_day, as (date, weekday, through_trips), adds the previous
    service day's trains that run after midnight. GTFS writes those as
    24:10:00 and so on under the previous day's services, so a cache built
    for today alone would lose every train between midnight and the first
    of today's. They are shifted to read from today's midnight.
    """
    route_prefixes = tuple(route_ids)
    days = [(_active_service_ids(metro_z, today_str, dow), through_trips or {}, 0, 0)]
    if previous_day:
        p_date, p_dow, p_through = previous_day
        days.append((_active_service_ids(metro_z, p_date, p_dow), p_through or {},
                     -DAY_SECONDS, DAY_SECONDS))

    # Trip -> direction, per service day
    # Prefix match so route ID variants (e.g. "aus:vic:vic-02-HBE:E" express)
    # are included; base IDs end with ":" so "-R:" replacement buses never match.
    day_dirs: list[dict[str, int]] = [{} for _ in days]
    tunnel_trips: set[str] = set()
    with metro_z.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            sid = row.get("service_id")
            rid = row.get("route_id", "")
            direction = int(row.get("direction_id", 0))
            kept = False
            for (active, through, _off, _min), dirs in zip(days, day_dirs):
                if sid not in active:
                    continue
                if rid.startswith(route_prefixes):
                    dirs[row["trip_id"]] = direction
                elif row["trip_id"] in through:
                    dirs[row["trip_id"]] = 1 - direction
                else:
                    continue
                kept = True
            if kept and any(rid.startswith(t) for t in TUNNEL_SIDES):
                tunnel_trips.add(row["trip_id"])
    wanted = set().union(*day_dirs)

    # Departures at our stops. A tunnel trip's last stop is the Town Hall
    # handover, where the train carries on as the next trip — that row is an
    # arrival, and keeping it would list every train there twice.
    rows = []
    last_seq: dict[str, int] = {}
    with metro_z.open("stop_times.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            tid = row.get("trip_id")
            if tid in tunnel_trips:
                seq = int(row.get("stop_sequence") or 0)
                if seq > last_seq.get(tid, -1):
                    last_seq[tid] = seq
            if row.get("stop_id") in stop_ids and tid in wanted:
                rows.append(row)

    deps = []
    for row in rows:
        tid = row["trip_id"]
        if tid in tunnel_trips and int(row.get("stop_sequence") or 0) == last_seq.get(tid):
            continue
        dep_str = row.get("departure_time","") or row.get("arrival_time","")
        if not dep_str:
            continue
        try:
            h, m, s = dep_str.split(":")
            dep_sec = int(h)*3600 + int(m)*60 + int(s)
        except ValueError:
            continue
        for (_a, _t, offset, min_sec), dirs in zip(days, day_dirs):
            if tid not in dirs or dep_sec < min_sec:
                continue
            deps.append({
                # Seconds from the cache day's midnight; may exceed 86400.
                "dep_seconds": dep_sec + offset,
                "direction_id": dirs[tid],
                "trip_id": tid,
                "stop_id": row["stop_id"],
                # Lets realtime updates be matched to this stop even when they
                # name a different platform's stop_id.
                "stop_sequence": int(row.get("stop_sequence") or 0),
                "dep_str": dep_str,
                "previous_day": offset != 0,
            })
    deps.sort(key=lambda x: x["dep_seconds"])
    return deps


# ---------------------------------------------------------------------- #
# Finding and downloading the current release
# ---------------------------------------------------------------------- #

def resolve_gtfs_release() -> dict:
    """Look up the current timetable release on the open data portal.

    Returns {"url", "resource_id", "last_modified", "size", "version"}.
    The resource ID is not stable (it changed on 2026-11-02, which left the
    old hardcoded link returning 404), and a weekly update can also replace
    the file under the same ID, so the version covers all three.
    """
    resp = requests.get(
        GTFS_PACKAGE_API,
        timeout=GTFS_API_TIMEOUT,
        headers={"User-Agent": GTFS_USER_AGENT},
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"catalogue lookup unsuccessful: {body.get('error')}")
    resources = [
        r for r in (body.get("result") or {}).get("resources", [])
        if r.get("url")
        and r.get("state", "active") == "active"
        and (str(r.get("format", "")).upper() == "ZIP"
             or r["url"].lower().endswith(".zip"))
    ]
    if not resources:
        raise RuntimeError("catalogue lists no GTFS zip")
    newest = max(
        resources, key=lambda r: r.get("last_modified") or r.get("created") or "")
    modified = newest.get("last_modified") or newest.get("created") or ""
    return {
        "url": newest["url"],
        "resource_id": newest.get("id", ""),
        "last_modified": modified,
        "size": newest.get("size"),
        "version": f"{newest.get('id', '')}|{modified}|{newest.get('size')}",
    }


def _store_dir(config_dir: str) -> Path:
    return Path(config_dir) / GTFS_STORE_DIR


def _mode_zip_path(config_dir: str, mode: str) -> Path:
    return _store_dir(config_dir) / f"{mode}.zip"


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as err:
        _LOGGER.warning("Could not read %s: %s", path.name, err)
        return None


def _write_json(path: Path, data: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _remove_leftovers(config_dir: str) -> None:
    """Delete temporary files a download interrupted mid-way left behind.

    A restart or power cut during a download skips its own clean-up, which
    could leave up to ~300 MB on disk until the next release. Only runs when
    no download is under way (the lock is free), so it never deletes the
    files of one in progress.
    """
    store = _store_dir(config_dir)
    if not store.is_dir() or not _DOWNLOAD_LOCK.acquire(blocking=False):
        return
    try:
        for leftover in store.glob("*.tmp"):
            try:
                size = leftover.stat().st_size
                leftover.unlink()
                _LOGGER.info("Removed leftover %s (%d KB)", leftover.name, size // 1024)
            except OSError as err:
                _LOGGER.warning("Could not remove leftover %s: %s", leftover.name, err)
    finally:
        _DOWNLOAD_LOCK.release()


def _download_release(url: str, config_dir: str) -> tuple[set[str], dict[str, str]]:
    """Download a release and keep each mode's inner timetable on disk.

    Streams to a temporary file rather than holding ~300 MB in memory, and
    keeps every mode so a tram entry reuses a train entry's download. Each
    mode's timetable is checked (its checksum while unpacking, then that it
    opens and has stop times) and only then swapped in, atomically, over the
    previous one. Returns (modes replaced, {mode: why it wasn't}); the
    caller records each mode's version separately, so one mode failing
    never marks the other's old timetable as current.
    """
    store = _store_dir(config_dir)
    store.mkdir(parents=True, exist_ok=True)
    tmp = store / "download.zip.tmp"
    replaced: set[str] = set()
    failed: dict[str, str] = {}
    _LOGGER.info("Downloading GTFS timetable from %s", url)
    try:
        with requests.get(
            url,
            stream=True,
            timeout=GTFS_DOWNLOAD_TIMEOUT,
            headers={"User-Agent": GTFS_USER_AGENT},
        ) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        _LOGGER.info("Downloaded %d KB", tmp.stat().st_size // 1024)

        with zipfile.ZipFile(tmp) as outer:
            for mode in MODES:
                part = store / f"{mode}.zip.tmp"
                try:
                    entry = _find_mode_entry(outer, mode)
                    with outer.open(entry) as src, open(part, "wb") as dst:
                        shutil.copyfileobj(src, dst, 1 << 20)
                    with zipfile.ZipFile(part) as check:
                        if "stop_times.txt" not in check.namelist():
                            raise RuntimeError(f"{entry} has no stop_times.txt")
                    os.replace(part, _mode_zip_path(config_dir, mode))
                    replaced.add(mode)
                except Exception as err:
                    part.unlink(missing_ok=True)
                    failed[mode] = str(err) or type(err).__name__
                    _LOGGER.warning("Kept the previous %s timetable: %s", mode, failed[mode])
    finally:
        tmp.unlink(missing_ok=True)
    return replaced, failed


def _mode_versions(on_disk: dict) -> dict[str, dict]:
    """{mode: release info} from release.json, reading the old layout too.

    The first layout had one version for all modes plus a list of modes;
    each mode now records its own, as they can be replaced separately.
    """
    modes = on_disk.get("modes", {})
    if isinstance(modes, list):
        shared = {k: on_disk.get(k, "") for k in
                  ("version", "url", "resource_id", "last_modified", "downloaded_at")}
        return {m: dict(shared) for m in modes}
    return dict(modes)


def _service_end(z: zipfile.ZipFile) -> str:
    """Last date (YYYYMMDD) the timetable has any services for."""
    end = ""
    with z.open("calendar.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            end = max(end, row.get("end_date", ""))
    with z.open("calendar_dates.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            if row.get("exception_type") == "1":
                end = max(end, row.get("date", ""))
    return end


def _without_departures(cache: dict, error: str) -> dict:
    """An old cache stripped of anything tied to the day it was built.

    Its stop list still resolves stations, but its departures belong to
    another day (a Monday timetable on a Sunday, say), so showing them would
    be worse than showing realtime departures alone.
    """
    out = dict(cache)
    out["scheduled_by_station"] = {k: [] for k in cache.get("scheduled_by_station", {})}
    out["through_trips"] = {}
    out["timetable_error"] = error
    return out


# ---------------------------------------------------------------------- #
# Building the day's cache
# ---------------------------------------------------------------------- #

def download_and_cache_gtfs(
    config_dir: str,
    station_names: list[str],
    route_ids: set[str],
    mode: str = MODE_TRAIN,
    check_release: bool = True,
) -> dict:
    """Return today's timetable cache, downloading a new release if needed.

    Once a day (the first call on a new date) this asks the portal for the
    current release, downloads it only if it differs from the one on disk,
    and rebuilds today's departures from the timetable on disk. Failures
    fall back to the timetable already on disk, which stays correct for
    any date it covers; failing that, to an old cache without departures.

    check_release=False uses only what is on disk, with no network access,
    so Home Assistant's startup never waits on the portal or a download;
    the coordinator runs the check in the background afterwards.
    """
    # Train and tram are separate config entries, so they need separate caches.
    cache_name = GTFS_CACHE_FILE if mode == MODE_TRAIN else \
        GTFS_CACHE_FILE.replace(".json", f"_{mode}.json")
    cache_path = Path(config_dir) / cache_name
    now = _local_now()
    today_str = now.strftime("%Y%m%d")
    now_utc = datetime.now(timezone.utc).isoformat()

    old = _read_json(cache_path)
    if (old and old.get("schema") == GTFS_CACHE_SCHEMA
            and old.get("cached_date") == today_str
            and (old.get("release_checked_date") == today_str or not check_release)
            and all(s.lower() in old.get("scheduled_by_station", {}) for s in station_names)):
        _LOGGER.debug("Using today's GTFS cache")
        return old
    old = old or {}

    release_path = _store_dir(config_dir) / GTFS_RELEASE_FILE
    zip_path = _mode_zip_path(config_dir, mode)

    release: dict | None = None
    check_error = ""
    if check_release:
        try:
            release = resolve_gtfs_release()
        except Exception as err:
            check_error = f"Could not look up the current timetable: {err}"
            _LOGGER.warning("%s", check_error)

    download_error = ""
    _remove_leftovers(config_dir)
    with _DOWNLOAD_LOCK:
        versions = _mode_versions(_read_json(release_path) or {})
        have_zip = zip_path.exists() and mode in versions
        target = release or (
            None if have_zip or not check_release
            else {"url": GTFS_FALLBACK_URL, "version": ""})
        if target and (not have_zip or target["version"] != versions[mode].get("version")):
            try:
                replaced, failed = _download_release(target["url"], config_dir)
                for m in replaced:
                    versions[m] = {
                        "version": target["version"],
                        "url": target["url"],
                        "resource_id": target.get("resource_id", ""),
                        "last_modified": target.get("last_modified", ""),
                        "downloaded_at": now_utc,
                    }
                if replaced:
                    _write_json(release_path, {"modes": versions})
                if mode in failed:
                    raise RuntimeError(f"{mode} timetable unusable: {failed[mode]}")
            except Exception as err:
                download_error = f"Timetable download failed: {err}"
                _LOGGER.warning("%s", download_error)
            # A failed swap can't leave a half-written zip (os.replace is
            # atomic), so whatever is on disk is still a whole timetable.
            have_zip = zip_path.exists() and mode in versions
        elif release:
            _LOGGER.debug("Timetable on disk is current (%s)", release["last_modified"])
    on_disk = versions.get(mode, {})

    error = download_error or check_error
    if not have_zip:
        if not check_release:
            raise RuntimeError("No timetable on disk yet")
        if old.get("stops"):
            _LOGGER.warning("No timetable on disk; scheduled departures unavailable")
            return _without_departures(old, error)
        raise RuntimeError(error or "No GTFS timetable available")

    with zipfile.ZipFile(zip_path) as metro_z:
        stops = _load_stops(metro_z, mode)
        direction_labels = _load_direction_labels(metro_z, route_ids)
        service_end = _service_end(metro_z)

        dow = now.strftime("%A").lower()
        active_sids = _active_service_ids(metro_z, today_str, dow)
        today_through = _through_trips(metro_z, route_ids, active_sids)
        yday = now - timedelta(days=1)
        y_str, y_dow = yday.strftime("%Y%m%d"), yday.strftime("%A").lower()
        y_through = _through_trips(
            metro_z, route_ids, _active_service_ids(metro_z, y_str, y_dow))
        # Realtime trips are looked up in through_trips by trip_id, so
        # yesterday's after-midnight continuations need to be in it too.
        through_trips = {**y_through, **today_through}

        # Load scheduled departures for each requested station
        scheduled_by_station: dict[str, list] = {}
        for station_name in station_names:
            key = station_name.lower()
            entry = stops.get(key)
            if not entry:
                for k, v in stops.items():
                    if key in k:
                        entry = v
                        break
            if entry:
                stop_ids = set(entry["stop_ids"])
                deps = _load_scheduled_departures(
                    metro_z, stop_ids, route_ids, today_str, dow,
                    today_through,
                    previous_day=(y_str, y_dow, y_through))
                scheduled_by_station[key] = deps
                _LOGGER.info("Loaded %d departures for %s", len(deps), station_name)
            else:
                _LOGGER.warning("Station '%s' not found in GTFS static data", station_name)
                scheduled_by_station[key] = []

    if not active_sids:
        error = error or f"The timetable has no services for {today_str} (it runs to {service_end})"
        _LOGGER.warning("%s", error)

    cache = {
        "cached_at": now_utc,
        "cached_date": today_str,
        "schema": GTFS_CACHE_SCHEMA,
        "mode": mode,
        "stops": stops,
        "direction_labels": direction_labels,
        "through_trips": through_trips,
        "scheduled_by_station": scheduled_by_station,
        # Release bookkeeping, for the staleness repair issue.
        "release": on_disk,
        "release_check_ok": release is not None,
        # The day the portal was last asked (successfully or not); the
        # coordinator checks in the background when this isn't today.
        "release_checked_date": today_str if check_release else old.get("release_checked_date", ""),
        "release_checked_at": now_utc if release else old.get("release_checked_at", ""),
        "service_end": service_end,
        "has_service_today": bool(active_sids),
        "timetable_error": error,
    }

    try:
        _write_json(cache_path, cache)
        _LOGGER.info("GTFS cache saved")
    except Exception as err:
        _LOGGER.warning("Could not save GTFS cache: %s", err)

    return cache


def get_stop_ids(cache: dict, station_name: str) -> list[str]:
    stops = cache.get("stops", {})
    key = station_name.lower()
    if key in stops:
        return stops[key]["stop_ids"]
    for k, v in stops.items():
        if key in k:
            return v["stop_ids"]
    return []


def get_all_station_names(cache: dict) -> list[str]:
    return sorted(v["stop_name"] for v in cache.get("stops", {}).values())


def get_direction_label(cache: dict, route_id: str, direction_id, fallback: str = "") -> str:
    """Destination for a route's direction, e.g. "Box Hill" for tram 109 dir 0."""
    labels = cache.get("direction_labels", {})
    entry = labels.get(route_id)
    if entry is None:
        for rid, val in labels.items():
            if rid.startswith(route_id):
                entry = val
                break
    if not entry:
        return fallback
    return entry.get(str(direction_id), fallback)


def get_next_n_scheduled(
    cache: dict,
    station_name: str,
    direction_id: int,
    after_seconds: int,
    count: int = 4,
    exclude_trips: set[str] | frozenset = frozenset(),
) -> list[dict]:
    """Up to `count` scheduled departures in a direction after after_seconds.

    exclude_trips drops trips the realtime feed says won't stop here
    (cancelled, or skipping this station).
    """
    key = station_name.lower()
    all_deps = cache.get("scheduled_by_station", {}).get(key, [])
    results = []
    for dep in all_deps:
        if dep.get("trip_id") in exclude_trips:
            continue
        if dep["direction_id"] == direction_id and dep["dep_seconds"] > after_seconds:
            results.append(dep)
            if len(results) >= count:
                break
    return results

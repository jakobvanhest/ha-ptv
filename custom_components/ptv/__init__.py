"""PTV - Home Assistant Integration."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
from google.transit import gtfs_realtime_pb2

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.util import dt as dt_util
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    API_BASE_URL, API_HEADER_KEY,
    API_SERVICE_ALERTS_PATH, API_TRIP_UPDATES_PATH, API_VEHICLE_POSITIONS_PATH,
    CONF_API_KEY, CONF_LINES, CONF_SCAN_INTERVAL, CONF_STATIONS,
    CONF_STATION_NAME, CONF_STATION_DIRECTIONS, CONF_STATION_COUNT,
    CONF_ALERT_EFFECTS, DEFAULT_ALERT_EFFECTS,
    CONF_OFFPEAK_INTERVAL, CONF_OVERNIGHT_INTERVAL,
    CONF_PEAK_AM_START, CONF_PEAK_AM_END, CONF_PEAK_PM_START, CONF_PEAK_PM_END,
    CONF_OVERNIGHT_START, CONF_OVERNIGHT_END, CONF_PEAK_ON_WEEKENDS,
    DEFAULT_OFFPEAK_INTERVAL, DEFAULT_OVERNIGHT_INTERVAL,
    DEFAULT_PEAK_AM_START, DEFAULT_PEAK_AM_END,
    DEFAULT_PEAK_PM_START, DEFAULT_PEAK_PM_END,
    DEFAULT_OVERNIGHT_START, DEFAULT_OVERNIGHT_END, DEFAULT_PEAK_ON_WEEKENDS,
    DEFAULT_DELAY_THRESHOLD, DEFAULT_SCAN_INTERVAL, DEFAULT_STATION_COUNT,
    DIR_CITYBOUND, DIR_OUTBOUND, DIR_BOTH,
    CONF_MODE, DEFAULT_MODE, MODE_TRAIN, api_base_for_mode, mode_info, routes_for_mode,
    line_phrase, TUNNEL_SIDES,
    DOMAIN, GTFS_STALE_DAYS, KNOWN_STOP_IDS, METRO_LINES, OCCUPANCY_LABELS,
    PROBLEM_NONE, PROBLEM_DELAYS, PROBLEM_ALERT, PROBLEM_CANCELLATIONS, PROBLEM_MAJOR,
)
from .gtfs_static import download_and_cache_gtfs, get_next_n_scheduled, get_stop_ids

_LOGGER = logging.getLogger(__name__)
PLATFORMS = ["binary_sensor", "sensor"]

# Overnight works are published as one active_period spanning the whole
# campaign (e.g. Mon 23:50 -> Thu 23:59) even though the disruption only runs
# from that start time until the last service each night. The start's
# time-of-day is the nightly start; the text says "each night".
OVERNIGHT_MIN_SPAN = timedelta(hours=24)
OVERNIGHT_PHRASES = ("each night", "every night", "each evening")
# Trains have finished by this hour, so the nightly window is start -> 4am.
END_OF_SERVICE_HOUR = 4
# Per-request limits for the realtime feeds: connect, then the whole read.
FEED_TIMEOUT = aiohttp.ClientTimeout(total=35, connect=10)


class FeedError(Exception):
    """A realtime feed request failed (network, HTTP status or rate limit)."""


# How old a feed may be before a failed request fails the update.
FEED_MAX_STALE = timedelta(minutes=15)
# Pause before asking an endpoint again after it answers 429 Too Many
# Requests without a Retry-After header. PTV allows 20-27 calls a minute
# per endpoint; a normal poll makes one call to each.
RATE_LIMIT_BACKOFF = timedelta(seconds=90)
# Wait between attempts when the daily timetable refresh fails.
GTFS_REFRESH_RETRY = timedelta(hours=1)
# How long a train that drops out of the live feed keeps its last live time.
# PTV's trip updates intermittently leave out trains that are still running,
# sometimes for several polls in a row; without this the next-train sensors
# jump between trains and a delayed train can vanish before it departs.
REALTIME_HOLD = timedelta(minutes=15)


def _timetable_problem(cache: dict, now: datetime) -> str:
    """Why the timetable can't be trusted, or "" when it is fine."""
    if not cache:
        return "No timetable has been downloaded yet."
    if cache.get("cached_date") != now.strftime("%Y%m%d"):
        return cache.get("timetable_error") or "Today's timetable could not be loaded."
    if not cache.get("has_service_today", True):
        return cache.get("timetable_error") or "The timetable has no services for today."
    confirmed = (cache.get("release_checked_at")
                 or (cache.get("release") or {}).get("downloaded_at"))
    if not confirmed:
        return cache.get("timetable_error") or "The timetable version has never been confirmed."
    age = now - datetime.fromisoformat(confirmed)
    if age > timedelta(days=GTFS_STALE_DAYS):
        return (f"No new timetable has been confirmed for {age.days} days. "
                f"{cache.get('timetable_error', '')}").strip()
    return ""


def _issue_id(entry_id: str) -> str:
    return f"timetable_stale_{entry_id}"


def update_timetable_issue(hass: HomeAssistant, entry: ConfigEntry, cache: dict,
                           pending: bool = False) -> None:
    """Raise or clear the repair issue for an out-of-date timetable.

    Before this, a broken download only logged a warning, and the sensors
    quietly kept showing an old day's timetable for weeks. With pending (a
    background load is under way) an existing issue is left as it is and
    no new one is raised until that load has finished.
    """
    problem = _timetable_problem(cache, dt_util.now())
    if problem and pending:
        return
    if not problem:
        ir.async_delete_issue(hass, DOMAIN, _issue_id(entry.entry_id))
        return
    ir.async_create_issue(
        hass, DOMAIN, _issue_id(entry.entry_id),
        is_fixable=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key="timetable_stale",
        translation_placeholders={"title": entry.title, "reason": problem},
    )


def _overnight_window(alert_text: str, start_ts: int, end_ts: int) -> str | None:
    """Return the nightly start time (e.g. "11:50pm") for overnight works.

    Returns None when the alert is a genuinely continuous disruption.
    """
    if not start_ts or not end_ts:
        return None
    if timedelta(seconds=end_ts - start_ts) < OVERNIGHT_MIN_SPAN:
        return None
    if not any(p in alert_text.lower() for p in OVERNIGHT_PHRASES):
        return None
    start = dt_util.as_local(dt_util.utc_from_timestamp(start_ts))
    return start.strftime("%-I:%M%p").lower()


def _in_overnight_window(now: datetime, start_ts: int) -> bool:
    """True when `now` falls inside tonight's works window."""
    start = dt_util.as_local(dt_util.utc_from_timestamp(start_ts))
    start_minutes = start.hour * 60 + start.minute
    now_minutes = now.hour * 60 + now.minute
    return now_minutes >= start_minutes or now_minutes < END_OF_SERVICE_HOUR * 60


def _minutes(hhmmss: str) -> int:
    """Parse a "HH:MM:SS" selector value into minutes since midnight."""
    h, m, *_ = (hhmmss or "0:0:0").split(":")
    return int(h) * 60 + int(m)


def _within(now_min: int, start_min: int, end_min: int) -> bool:
    """True if now_min is in [start, end), handling windows spanning midnight."""
    if start_min <= end_min:
        return start_min <= now_min < end_min
    return now_min >= start_min or now_min < end_min


POLLING_DEFAULTS = {
    CONF_SCAN_INTERVAL: DEFAULT_SCAN_INTERVAL,
    CONF_OFFPEAK_INTERVAL: DEFAULT_OFFPEAK_INTERVAL,
    CONF_OVERNIGHT_INTERVAL: DEFAULT_OVERNIGHT_INTERVAL,
    CONF_PEAK_AM_START: DEFAULT_PEAK_AM_START,
    CONF_PEAK_AM_END: DEFAULT_PEAK_AM_END,
    CONF_PEAK_PM_START: DEFAULT_PEAK_PM_START,
    CONF_PEAK_PM_END: DEFAULT_PEAK_PM_END,
    CONF_OVERNIGHT_START: DEFAULT_OVERNIGHT_START,
    CONF_OVERNIGHT_END: DEFAULT_OVERNIGHT_END,
    CONF_PEAK_ON_WEEKENDS: DEFAULT_PEAK_ON_WEEKENDS,
}


def polling_config(entry: ConfigEntry) -> dict:
    """Options override setup data, which overrides the defaults."""
    return {
        key: entry.options.get(key, entry.data.get(key, default))
        for key, default in POLLING_DEFAULTS.items()
    }


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    api_key = entry.data[CONF_API_KEY]
    monitored_lines = entry.data[CONF_LINES]
    mode = entry.data.get(CONF_MODE, DEFAULT_MODE)
    polling = polling_config(entry)
    scan_interval = polling[CONF_SCAN_INTERVAL]
    alert_effects = set(int(e) for e in entry.data.get(CONF_ALERT_EFFECTS, DEFAULT_ALERT_EFFECTS))
    stations_config = entry.data.get(CONF_STATIONS, [])

    routes = routes_for_mode(mode)
    route_ids = {routes[l] for l in monitored_lines if l in routes}
    station_names = [s[CONF_STATION_NAME] for s in stations_config]

    # Resolve stop IDs for each station
    station_stop_ids: dict[str, dict] = {}
    gtfs_cache: dict = {}

    if station_names:
        # Hardcoded fast path for known stations (train only)
        if mode == MODE_TRAIN:
            for name in station_names:
                key = name.lower()
                if key in KNOWN_STOP_IDS:
                    station_stop_ids[key] = KNOWN_STOP_IDS[key]
                    _LOGGER.info("Using hardcoded stop_ids for '%s'", name)

        # Timetable for today and any stations not hardcoded, from disk
        # only: checking the portal (and any ~300 MB download) happens in
        # the background once setup is done, so startup never waits on it.
        try:
            gtfs_cache = await hass.async_add_executor_job(
                download_and_cache_gtfs,
                hass.config.config_dir,
                station_names,
                route_ids,
                mode,
                False,
            )
            _resolve_stop_ids(gtfs_cache, station_names, station_stop_ids)
        except Exception as err:
            _LOGGER.info("No timetable on disk yet (%s); loading it in the background", err)

    coordinator = PTVDataCoordinator(
        hass,
        entry=entry,
        api_key=api_key,
        monitored_lines=monitored_lines,
        scan_interval=scan_interval,
        alert_effects=alert_effects,
        stations_config=stations_config,
        station_stop_ids=station_stop_ids,
        gtfs_cache=gtfs_cache,
        polling=polling,
        mode=mode,
    )

    if station_names:
        update_timetable_issue(hass, entry, gtfs_cache, pending=True)
    await coordinator.async_config_entry_first_refresh()
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    coordinator = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if coordinator and coordinator._timetable_task and not coordinator._timetable_task.done():
        coordinator._timetable_task.cancel()
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    ir.async_delete_issue(hass, DOMAIN, _issue_id(entry.entry_id))


def _resolve_stop_ids(cache: dict, station_names: list[str], station_stop_ids: dict) -> None:
    """Fill in stop IDs from the timetable for stations not hardcoded."""
    for name in station_names:
        key = name.lower()
        if key in station_stop_ids:
            continue
        ids = get_stop_ids(cache, name)
        if ids:
            station_stop_ids[key] = {"stop_ids": ids}
            _LOGGER.info("Resolved '%s' stop_ids from GTFS: %s", name, ids)
        else:
            _LOGGER.warning("Could not resolve stop_ids for '%s'", name)


class PTVDataCoordinator(DataUpdateCoordinator):

    def __init__(self, hass, api_key, monitored_lines, scan_interval,
                 alert_effects, stations_config, station_stop_ids, gtfs_cache,
                 polling=None, mode=DEFAULT_MODE, entry=None):
        self.entry = entry
        self.api_key = api_key
        self.mode = mode
        self.api_base = api_base_for_mode(mode)
        self._last_feeds: dict[str, tuple[bytes, datetime]] = {}
        self._stale_feeds: set[str] = set()
        self.monitored_lines = monitored_lines
        self.alert_effects = alert_effects        # set of int effect values to include
        self.stations_config = stations_config   # list of {name, directions, count}
        self.station_stop_ids = station_stop_ids  # key -> {stop_ids, citybound_stop_ids, outbound_stop_ids}
        self.gtfs_cache = gtfs_cache or {}
        self._gtfs_retry_after: datetime | None = None
        self._timetable_task: asyncio.Task | None = None
        # Home Assistant's shared HTTP session (connection reuse, its proxy
        # and SSL settings); the realtime feeds no longer use worker threads.
        self._session = async_get_clientsession(hass)
        self._rate_limited_until: dict[str, datetime] = {}
        # (line, station key, direction_id) -> trip_id -> (last live hit, seen at)
        self._rt_seen: dict[tuple, dict[str, tuple[dict, datetime]]] = {}
        self.polling = {
            CONF_SCAN_INTERVAL: scan_interval,
            CONF_OFFPEAK_INTERVAL: DEFAULT_OFFPEAK_INTERVAL,
            CONF_OVERNIGHT_INTERVAL: DEFAULT_OVERNIGHT_INTERVAL,
            CONF_PEAK_AM_START: DEFAULT_PEAK_AM_START,
            CONF_PEAK_AM_END: DEFAULT_PEAK_AM_END,
            CONF_PEAK_PM_START: DEFAULT_PEAK_PM_START,
            CONF_PEAK_PM_END: DEFAULT_PEAK_PM_END,
            CONF_OVERNIGHT_START: DEFAULT_OVERNIGHT_START,
            CONF_OVERNIGHT_END: DEFAULT_OVERNIGHT_END,
            CONF_PEAK_ON_WEEKENDS: DEFAULT_PEAK_ON_WEEKENDS,
            **(polling or {}),
        }
        super().__init__(
            hass, _LOGGER, name=DOMAIN,
            update_interval=self._interval_for_now(),
        )

    def _interval_for_now(self) -> timedelta:
        """Poll fast during commute peaks, slowly overnight."""
        p = self.polling
        now = dt_util.now()
        now_min = now.hour * 60 + now.minute

        peak_applies = p[CONF_PEAK_ON_WEEKENDS] or now.weekday() < 5
        if peak_applies and (
            _within(now_min, _minutes(p[CONF_PEAK_AM_START]), _minutes(p[CONF_PEAK_AM_END]))
            or _within(now_min, _minutes(p[CONF_PEAK_PM_START]), _minutes(p[CONF_PEAK_PM_END]))
        ):
            return timedelta(seconds=int(p[CONF_SCAN_INTERVAL]))

        if _within(now_min, _minutes(p[CONF_OVERNIGHT_START]), _minutes(p[CONF_OVERNIGHT_END])):
            return timedelta(seconds=int(p[CONF_OVERNIGHT_INTERVAL]))

        return timedelta(seconds=int(p[CONF_OFFPEAK_INTERVAL]))

    def _cache_midnight(self) -> datetime:
        """Local midnight of the day the timetable cache was built for.

        Scheduled times count seconds from it, and can run past 86400 for
        trains after midnight. Between midnight and the day's refresh the
        cache is still yesterday's, and its times stay correct this way.
        """
        day = (self.gtfs_cache or {}).get("cached_date", "")
        try:
            # Home Assistant's time zone, with that date's own UTC offset
            # (daylight saving can differ from today's).
            return dt_util.start_of_local_day(datetime.strptime(day, "%Y%m%d").date())
        except ValueError:
            return dt_util.start_of_local_day()

    def _secs_since_midnight(self) -> int:
        """Seconds from the cache day's midnight to now."""
        return int((dt_util.now() - self._cache_midnight()).total_seconds())

    def _secs_to_dt(self, secs: int) -> datetime:
        return self._cache_midnight() + timedelta(seconds=secs)

    async def _async_fetch_feed(self, path: str) -> bytes:
        """Fetch a feed, falling back to its last good copy on a blip.

        The PTV API occasionally stalls on a single request. Failing the
        whole update for that marks every entity unavailable, which breaks
        any automation that happens to run then. A copy up to
        FEED_MAX_STALE old is still accurate enough: the parsers drop
        departures already in the past.
        """
        now = datetime.now(timezone.utc)
        try:
            until = self._rate_limited_until.get(path)
            if until and now < until:
                # Don't ask again until PTV said to; asking anyway only
                # extends the block.
                raise FeedError(f"rate limited until {dt_util.as_local(until):%H:%M:%S}")
            try:
                async with self._session.get(
                    f"{self.api_base}{path}",
                    headers={API_HEADER_KEY: self.api_key},
                    timeout=FEED_TIMEOUT,
                ) as resp:
                    if resp.status == 429:
                        self._rate_limited_until[path] = now + self._retry_after(resp)
                        _LOGGER.warning(
                            "PTV %s answered 429 Too Many Requests; pausing it until %s",
                            path,
                            dt_util.as_local(self._rate_limited_until[path]).strftime("%H:%M:%S"),
                        )
                    if resp.status >= 400:
                        raise FeedError(f"HTTP {resp.status}")
                    content = await resp.read()
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                raise FeedError(str(err) or type(err).__name__) from err
        except FeedError as err:
            cached = self._last_feeds.get(path)
            if cached and datetime.now(timezone.utc) - cached[1] < FEED_MAX_STALE:
                if path not in self._stale_feeds:
                    _LOGGER.warning(
                        "PTV %s request failed (%s); using data from %s ago",
                        path, err, datetime.now(timezone.utc) - cached[1],
                    )
                    self._stale_feeds.add(path)
                return cached[0]
            raise
        if path in self._stale_feeds:
            _LOGGER.info("PTV %s recovered", path)
            self._stale_feeds.discard(path)
        self._last_feeds[path] = (content, datetime.now(timezone.utc))
        return content

    @staticmethod
    def _retry_after(resp) -> timedelta:
        """The pause a 429 asks for, capped at 15 minutes."""
        try:
            secs = int(resp.headers.get("Retry-After", ""))
            return timedelta(seconds=max(1, min(secs, 900)))
        except (TypeError, ValueError):
            return RATE_LIMIT_BACKOFF

    async def _async_fetch_all_feeds(self) -> tuple[bytes, bytes, bytes]:
        """The three feeds, fetched at the same time.

        Same calls as before (one per endpoint per poll, no retries), so no
        extra load against PTV's per-endpoint limits; a slow poll now takes
        as long as the slowest feed rather than all three added together.
        """
        trip, alerts, pos = await asyncio.gather(*(
            self._async_fetch_feed(path)
            for path in (API_TRIP_UPDATES_PATH, API_SERVICE_ALERTS_PATH,
                         API_VEHICLE_POSITIONS_PATH)
        ))
        return trip, alerts, pos

    def _schedule_timetable_refresh(self) -> None:
        """Start the timetable refresh in the background if it is due.

        Due once a day (the first poll after midnight rebuilds the day's
        departures and checks the portal for a new release), or on the first
        poll after startup, which loads from disk only. Running it as a
        task means polls, and Home Assistant's startup, never wait on the
        portal or a download.
        """
        if not self.stations_config:
            return
        if self._timetable_task and not self._timetable_task.done():
            return
        now = dt_util.now()
        today = now.strftime("%Y%m%d")
        cache = self.gtfs_cache or {}
        if cache.get("cached_date") == today and cache.get("release_checked_date") == today:
            return
        if self._gtfs_retry_after and now < self._gtfs_retry_after:
            return
        self._timetable_task = self.hass.async_create_background_task(
            self._async_refresh_timetable(), name="ptv timetable refresh")

    async def _async_refresh_timetable(self) -> None:
        """Reload the static timetable and check for a new release.

        It holds one day's services, so without this, departures after
        midnight would keep coming from the day Home Assistant started — a
        weekday timetable on a Saturday, for instance. Runs as a background
        task (see _schedule_timetable_refresh).
        """
        now = dt_util.now()
        routes = routes_for_mode(self.mode)
        names = [s[CONF_STATION_NAME] for s in self.stations_config]
        try:
            cache = await self.hass.async_add_executor_job(
                download_and_cache_gtfs,
                self.hass.config.config_dir,
                names,
                {routes[l] for l in self.monitored_lines if l in routes},
                self.mode,
            )
        except Exception as err:
            # Keep what we have, and don't retry the download on every poll.
            _LOGGER.warning("Timetable refresh failed: %s", err)
            self._gtfs_retry_after = now + GTFS_REFRESH_RETRY
            self._update_issue()
            return
        self._gtfs_retry_after = None
        if cache.get("cached_date") != now.strftime("%Y%m%d"):
            # No timetable for today (departures were dropped rather than
            # shown from another day); try again in a while.
            self._gtfs_retry_after = now + GTFS_REFRESH_RETRY
        self.gtfs_cache = cache
        # Stations that couldn't be resolved at setup (no timetable then).
        _resolve_stop_ids(cache, names, self.station_stop_ids)
        self._update_issue()
        _LOGGER.info("Timetable refreshed for %s", cache.get("cached_date"))
        # Show the new timetable now rather than at the next poll.
        await self.async_request_refresh()

    def _update_issue(self) -> None:
        if self.entry is not None:
            update_timetable_issue(self.hass, self.entry, self.gtfs_cache)

    async def _async_update_data(self) -> dict:
        # _schedule_refresh re-reads this after every poll, so retuning here
        # takes effect from the next one.
        interval = self._interval_for_now()
        if interval != self.update_interval:
            _LOGGER.debug("Polling interval now %s", interval)
            self.update_interval = interval
        self._schedule_timetable_refresh()
        try:
            trip_raw, alerts_raw, pos_raw = await self._async_fetch_all_feeds()
            return self._parse_all(trip_raw, alerts_raw, pos_raw)
        except FeedError as err:
            raise UpdateFailed(f"PTV API error: {err}") from err
        except Exception as err:
            raise UpdateFailed(f"Feed parse error: {err}") from err

    # ------------------------------------------------------------------ #
    # Data structure helpers
    # ------------------------------------------------------------------ #

    def _empty_station_data(self, station_cfg: dict) -> dict:
        name = station_cfg[CONF_STATION_NAME]
        directions = station_cfg.get(CONF_STATION_DIRECTIONS, DIR_BOTH)
        count = station_cfg.get(CONF_STATION_COUNT, DEFAULT_STATION_COUNT)
        return {
            "name": name,
            "directions": directions,
            "count": count,
            # Next N trains per direction: list of {dt_iso, delay_sec, is_realtime}
            "citybound": [],
            "outbound": [],
        }

    def _empty_line_data(self) -> dict:
        return {
            # Trip-level
            "delayed_trips": [],
            "cancelled_trips": [],
            "max_delay_seconds": 0,
            "delay_count": 0,
            "has_delays": False,
            "has_cancellations": False,
            # Alerts
            "alerts": [],
            "alert_count": 0,
            "live_alert_count": 0,
            "alert_text": "No active alerts",
            "live_alert_text": "",
            "overnight_works_text": "",
            # Vehicles
            "train_count": 0,
            "worst_occupancy": "Unknown",
            "worst_occupancy_int": -1,
            # Problem summary
            "problem_level": PROBLEM_NONE,
            "problem_summary": "Running normally",
            # Per-station data
            "stations": {},
        }

    # ------------------------------------------------------------------ #
    # Main parse
    # ------------------------------------------------------------------ #

    def _parse_all(self, trip_raw, alerts_raw, pos_raw) -> dict:
        routes = routes_for_mode(self.mode)
        route_map = {routes[l]: l for l in self.monitored_lines if l in routes}

        results = {line: self._empty_line_data() for line in self.monitored_lines}

        # Initialise per-station buckets
        for line in self.monitored_lines:
            for scfg in self.stations_config:
                key = scfg[CONF_STATION_NAME].lower()
                results[line]["stations"][key] = self._empty_station_data(scfg)

        self._parse_trip_updates(trip_raw, route_map, results)
        self._parse_service_alerts(alerts_raw, route_map, results)
        self._parse_vehicle_positions(pos_raw, route_map, results)
        self._apply_static_fallback(results)
        self._build_problem_summary(results)

        return results

    # ------------------------------------------------------------------ #
    # Trip updates
    # ------------------------------------------------------------------ #

    def _static_index(self) -> dict[str, dict[str, list[dict]]]:
        """station key -> trip_id -> that trip's scheduled stops there today.

        Built from the timetable, it supplies what the realtime feed may
        leave out: the trip's direction, the scheduled time (when an update
        carries only a delay) and the stop_sequence (when an update names a
        different platform's stop_id). Through-running trips already carry
        the direction relative to the selected line.
        """
        index: dict[str, dict[str, list[dict]]] = {}
        sched = (self.gtfs_cache or {}).get("scheduled_by_station", {})
        for scfg in self.stations_config:
            key = scfg[CONF_STATION_NAME].lower()
            by_trip: dict[str, list[dict]] = {}
            for dep in sched.get(key, []):
                if dep.get("trip_id"):
                    by_trip.setdefault(dep["trip_id"], []).append(dep)
            index[key] = by_trip
        return index

    def _station_hits(self, stus, direction_id, is_express, now_utc,
                      trip_id="", index=None, stats=None):
        """First future stop at each configured station for one trip.

        Returns ({station_key: hit, "skipped", "departed", or None}, the
        trip's worst delay). A hit records the direction to file it under: the
        timetable's when it knows the trip, otherwise the feed's.
        """
        index = index or {}
        stats = stats if stats is not None else {}
        hits: dict[str, dict | str | None] = {
            scfg[CONF_STATION_NAME].lower(): None for scfg in self.stations_config
        }
        midnight = None
        trip_max_delay = 0
        departed: set[str] = set()
        for stu in stus:
            delay = 0
            dep_time = None
            if stu.HasField("departure"):
                delay = stu.departure.delay or 0
                if stu.departure.time:
                    dep_time = datetime.fromtimestamp(stu.departure.time, tz=timezone.utc)
            elif stu.HasField("arrival"):
                delay = stu.arrival.delay or 0
                if stu.arrival.time:
                    dep_time = datetime.fromtimestamp(stu.arrival.time, tz=timezone.utc)
            trip_max_delay = max(trip_max_delay, delay)
            for key in hits:
                if hits[key] is not None:
                    continue  # already found for this trip
                scheduled = index.get(key, {}).get(trip_id, [])
                dir_id = scheduled[0]["direction_id"] if scheduled else direction_id
                info = self.station_stop_ids.get(key, {})
                # Use direction-specific stop IDs if available
                if dir_id == 1:
                    stop_ids = info.get("citybound_stop_ids", info.get("stop_ids", []))
                else:
                    stop_ids = info.get("outbound_stop_ids", info.get("stop_ids", []))
                match = None
                if stu.stop_id and stu.stop_id in stop_ids:
                    match = "stop_id"
                    sched = next((d for d in scheduled if d["stop_id"] == stu.stop_id), None) \
                        or (scheduled[0] if scheduled else None)
                else:
                    sched = next(
                        (d for d in scheduled
                         if stu.stop_sequence and d.get("stop_sequence") == stu.stop_sequence),
                        None,
                    )
                    if sched:
                        match = "stop_sequence"
                if not match:
                    continue
                if stu.schedule_relationship == 1:  # SKIPPED: not stopping here
                    hits[key] = "skipped"
                    stats["skipped"] = stats.get("skipped", 0) + 1
                    continue
                when = dep_time
                if when is None and sched:
                    # Delay-only update: scheduled time plus the delay.
                    if midnight is None:
                        # Scheduled seconds count from the cache day's
                        # midnight, after-midnight trains included.
                        midnight = self._cache_midnight()
                    when = midnight + timedelta(seconds=sched["dep_seconds"] + delay)
                    stats["time_from_delay"] = stats.get("time_from_delay", 0) + 1
                if when is None:
                    stats["no_time"] = stats.get("no_time", 0) + 1
                    continue
                if when <= now_utc:
                    departed.add(key)
                    continue
                stats[f"by_{match}"] = stats.get(f"by_{match}", 0) + 1
                if scheduled and dir_id != direction_id:
                    stats["direction_from_timetable"] = stats.get("direction_from_timetable", 0) + 1
                hits[key] = {
                    "actual": when, "delay_sec": delay, "is_express": is_express,
                    "dir": dir_id, "trip_id": trip_id,
                }
        for key in departed:
            if hits[key] is None:
                hits[key] = "departed"
        return hits, trip_max_delay

    def _parse_trip_updates(self, raw, route_map, results):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw)
        now_utc = datetime.now(timezone.utc)
        index = self._static_index()
        stats: dict[str, int] = {}

        # Buckets: line -> station_key -> direction_id -> list of {actual, delay_sec}
        rt_buckets: dict[str, dict[str, dict[int, list]]] = {}
        for line in self.monitored_lines:
            rt_buckets[line] = {}
            for scfg in self.stations_config:
                key = scfg[CONF_STATION_NAME].lower()
                rt_buckets[line][key] = {0: [], 1: []}
        # Trips the static fallback must not list: cancelled anywhere, or
        # skipping a given station.
        self._cancelled_trips: set[str] = set()
        self._skipped_at: dict[str, set[str]] = {
            scfg[CONF_STATION_NAME].lower(): set() for scfg in self.stations_config
        }
        # Trips whose live time at a station has already passed: not to be
        # filled back in from the timetable or held from an earlier poll.
        self._departed_at: dict[str, set[str]] = {
            scfg[CONF_STATION_NAME].lower(): set() for scfg in self.stations_config
        }

        # Tunnel trains are split at Town Hall: a Pakenham line's trains carry
        # on as Sunbury trips (and vice versa). Those continuations, linked by
        # block in the timetable, feed the line's station departures with
        # direction flipped — but not its delay or cancellation stats.
        through_trips = (self.gtfs_cache or {}).get("through_trips", {})

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update
            route_id = tu.trip.route_id
            line = route_map.get(route_id)
            is_express = False
            if line is None:
                # Also match route ID variants e.g. "aus:vic:vic-02-HBE:E" (express)
                for base_id, lname in route_map.items():
                    if route_id.startswith(base_id):
                        line = lname
                        is_express = True
                        break
            trip_id = tu.trip.trip_id
            continues_as = route_map.get(through_trips.get(trip_id, ""))
            continuing = [continues_as] if continues_as and continues_as != line else []
            if line is None and not continuing:
                continue
            direction_id = tu.trip.direction_id

            # 3=CANCELED, 5=DELETED
            if tu.trip.schedule_relationship in (3, 5):
                self._cancelled_trips.add(trip_id)
                if line is not None:
                    results[line]["cancelled_trips"].append(trip_id)
                    results[line]["has_cancellations"] = True
                continue
            stats["trips"] = stats.get("trips", 0) + 1

            # A citybound tunnel trip ends at the Town Hall handover, where the
            # train continues as another trip; that final stop is an arrival.
            stus = list(tu.stop_time_update)
            if direction_id == 1 and stus and any(route_id.startswith(r) for r in TUNNEL_SIDES):
                stus = stus[:-1]

            targets = ([(line, direction_id)] if line is not None else []) + \
                [(l, 1 - direction_id) for l in continuing]
            trip_max_delay = 0
            for tline, tdir in targets:
                hits, trip_max_delay = self._station_hits(
                    stus, tdir, is_express, now_utc,
                    trip_id=trip_id,
                    index=index, stats=stats,
                )
                for key, hit in hits.items():
                    if hit == "skipped":
                        self._skipped_at[key].add(trip_id)
                    elif hit == "departed":
                        self._departed_at[key].add(trip_id)
                    elif hit:
                        rt_buckets[tline][key][hit["dir"]].append(hit)

            if line is None:
                continue
            data = results[line]
            # Record line-level delay
            if trip_max_delay >= DEFAULT_DELAY_THRESHOLD:
                data["delayed_trips"].append({"trip_id": trip_id, "delay_seconds": trip_max_delay})
                data["delay_count"] += 1
                data["max_delay_seconds"] = max(data["max_delay_seconds"], trip_max_delay)
                data["has_delays"] = True

        # How realtime updates were matched, to diagnose stations that never
        # show live times (enable debug logging for custom_components.ptv).
        _LOGGER.debug("Realtime matching: %s", stats or "no trip updates for these lines")

        self._hold_missing_trips(rt_buckets, now_utc)

        # Sort and store realtime departures per station/direction
        for line in self.monitored_lines:
            for scfg in self.stations_config:
                key = scfg[CONF_STATION_NAME].lower()
                count = scfg.get(CONF_STATION_COUNT, DEFAULT_STATION_COUNT)
                for dir_id, dir_key in [(1, "citybound"), (0, "outbound")]:
                    hits = sorted(rt_buckets[line][key][dir_id], key=lambda x: x["actual"])
                    results[line]["stations"][key][dir_key] = [
                        {
                            "dt_iso": h["actual"].isoformat(),
                            "delay_sec": h["delay_sec"],
                            "is_realtime": True,
                            "is_express": h["is_express"],
                            "trip_id": h["trip_id"],
                        }
                        for h in hits[:count]
                    ]

    def _hold_missing_trips(self, rt_buckets, now_utc) -> None:
        """Keep trains that dropped out of this poll's feed at their last live time.

        A train seen live within REALTIME_HOLD that is missing now, and that
        the feed hasn't cancelled or marked as skipping the station, is put
        back with the time it last had, until that time passes. Trains seen
        in this poll replace what was remembered for them.
        """
        seen = getattr(self, "_rt_seen", None)
        if seen is None:
            seen = self._rt_seen = {}
        cancelled = getattr(self, "_cancelled_trips", set())
        skipped_at = getattr(self, "_skipped_at", {})
        departed_at = getattr(self, "_departed_at", {})
        held = 0
        for line, stations in rt_buckets.items():
            for key, by_dir in stations.items():
                for dir_id, hits in by_dir.items():
                    memory = seen.setdefault((line, key, dir_id), {})
                    present = {h["trip_id"] for h in hits if h.get("trip_id")}
                    for trip_id, (hit, seen_at) in list(memory.items()):
                        gone = (
                            hit["actual"] <= now_utc
                            or now_utc - seen_at > REALTIME_HOLD
                            or trip_id in cancelled
                            or trip_id in skipped_at.get(key, set())
                            or trip_id in departed_at.get(key, set())
                        )
                        if gone:
                            del memory[trip_id]
                        elif trip_id not in present:
                            hits.append(hit)
                            held += 1
                    for h in hits:
                        if h.get("trip_id") in present:
                            memory[h["trip_id"]] = (h, now_utc)
        if held:
            _LOGGER.debug("Kept %d train(s) missing from this poll at their last live time", held)

    # ------------------------------------------------------------------ #
    # Static timetable fallback
    # ------------------------------------------------------------------ #

    def _apply_static_fallback(self, results):
        if not self.gtfs_cache:
            return
        now_sec = self._secs_since_midnight()
        cancelled = getattr(self, "_cancelled_trips", set())
        skipped_at = getattr(self, "_skipped_at", {})
        departed_at = getattr(self, "_departed_at", {})

        for line in self.monitored_lines:
            for scfg in self.stations_config:
                key = scfg[CONF_STATION_NAME].lower()
                count = scfg.get(CONF_STATION_COUNT, DEFAULT_STATION_COUNT)
                directions = scfg.get(CONF_STATION_DIRECTIONS, DIR_BOTH)
                station_data = results[line]["stations"][key]

                for dir_id, dir_key in [(1, "citybound"), (0, "outbound")]:
                    if directions == DIR_CITYBOUND and dir_id == 0:
                        continue
                    if directions == DIR_OUTBOUND and dir_id == 1:
                        continue

                    existing = station_data[dir_key]
                    if len(existing) >= count:
                        continue

                    # Timetabled trains from now on that the live feed didn't
                    # list. Filling only after the last live train hid any
                    # train missing from the feed but due before it (a
                    # delayed train dropping out of the feed vanished). Seconds
                    # count from the cache day's midnight, so just after
                    # midnight this doesn't wrap to the morning's trains.
                    exclude = cancelled | skipped_at.get(key, set()) | \
                        departed_at.get(key, set()) | {
                        e.get("trip_id") for e in existing if e.get("trip_id")
                    }
                    scheduled = get_next_n_scheduled(
                        self.gtfs_cache, key, dir_id, now_sec, count, exclude
                    )
                    for sched in scheduled:
                        dt = self._secs_to_dt(sched["dep_seconds"])
                        existing.append({
                            "dt_iso": dt.isoformat(),
                            "delay_sec": 0,
                            "is_realtime": False,
                            "is_express": False,  # not derivable from static schedule
                            "trip_id": sched.get("trip_id", ""),
                        })
                    existing.sort(key=lambda e: datetime.fromisoformat(e["dt_iso"]))
                    del existing[count:]

    # ------------------------------------------------------------------ #
    # Service alerts
    # ------------------------------------------------------------------ #

    def _parse_service_alerts(self, raw, route_map, results):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw)
        now_ts = datetime.now(timezone.utc).timestamp()

        cause_labels = {
            1:"Unknown cause",2:"Other cause",3:"Technical problem",
            4:"Strike",5:"Demonstration",6:"Accident",7:"Holiday",
            8:"Weather",9:"Maintenance",10:"Construction",
            11:"Police activity",12:"Medical emergency",
        }
        effect_labels = {
            1:"No service",2:"Reduced service",3:"Significant delays",
            4:"Detour",5:"Additional service",6:"Modified service",
            7:"Other effect",8:"Unknown effect",9:"Stop moved",
            10:"No effect",11:"Accessibility issue",
        }

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            alert = entity.alert
            active = not alert.active_period or any(
                (p.start or 0) <= now_ts <= (p.end or float("inf"))
                for p in alert.active_period
            )
            if not active:
                continue

            if alert.effect not in self.alert_effects:
                continue

            def best_text(t):
                if not t.translation: return ""
                for tr in t.translation:
                    if tr.language in ("en","en-AU",""): return tr.text
                return t.translation[0].text

            header = best_text(alert.header_text)
            description = best_text(alert.description_text)

            # Overnight works stay listed outside their hours, but tagged with
            # the window and not counted as a live disruption.
            window_from = None
            in_window = True
            for p in alert.active_period:
                window_from = _overnight_window(
                    f"{header} {description}", p.start, p.end
                )
                if window_from:
                    in_window = _in_overnight_window(
                        dt_util.now(), p.start
                    )
                    break

            alert_dict = {
                "header": header,
                "description": description,
                "cause": cause_labels.get(alert.cause,""),
                "effect": effect_labels.get(alert.effect,""),
                "is_overnight_works": bool(window_from),
                "window_from": window_from or "",
                "window_to": "last service" if window_from else "",
                "active_now": in_window,
            }

            matched: set[str] = set()
            for informed in alert.informed_entity:
                rid = informed.route_id
                if rid in route_map:
                    matched.add(route_map[rid])
                elif rid:
                    for base_id, lname in route_map.items():
                        if rid.startswith(base_id):
                            matched.add(lname)
                elif not informed.trip.trip_id:
                    matched.update(self.monitored_lines)

            for line in matched:
                if line in results:
                    results[line]["alerts"].append(alert_dict)

        for line in self.monitored_lines:
            data = results[line]
            alerts = data["alerts"]
            data["alert_count"] = len(alerts)
            # Overnight works outside their hours shouldn't flag the line as
            # disrupted; they stay in alert_text, tagged with their window.
            data["live_alert_count"] = sum(1 for a in alerts if a["active_now"])

            texts: list[str] = []          # everything, works tagged with hours
            live_texts: list[str] = []     # happening now
            tonight_texts: list[str] = []  # works starting later tonight
            for a in alerts:
                text = a["header"] or a["description"]
                if a["effect"] and a["effect"] not in text:
                    text = f"{a['effect']}: {text}" if text else a["effect"]
                if not text:
                    continue
                if a["is_overnight_works"] and not a["active_now"]:
                    window = f"{a['window_from']}–{a['window_to']}"
                    tonight_texts.append(f"{window}: {text}")
                    texts.append(f"Tonight {window}: {text}")
                else:
                    live_texts.append(text)
                    texts.append(text)

            if alerts:
                data["alert_text"] = " | ".join(texts) if texts else "Active disruption"
            else:
                data["alert_text"] = "No active alerts"
            data["live_alert_text"] = " | ".join(live_texts)
            data["overnight_works_text"] = " | ".join(tonight_texts)

    # ------------------------------------------------------------------ #
    # Vehicle positions
    # ------------------------------------------------------------------ #

    def _parse_vehicle_positions(self, raw, route_map, results):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw)
        counts = {l: 0 for l in self.monitored_lines}
        worst = {l: -1 for l in self.monitored_lines}

        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vp = entity.vehicle
            rid = vp.trip.route_id
            line = route_map.get(rid)
            if line is None:
                for base_id, lname in route_map.items():
                    if rid.startswith(base_id):
                        line = lname
                        break
            if line is None:
                continue
            counts[line] += 1
            if vp.occupancy_status > worst[line]:
                worst[line] = vp.occupancy_status

        for line in self.monitored_lines:
            results[line]["train_count"] = counts[line]
            occ = worst[line]
            results[line]["worst_occupancy"] = OCCUPANCY_LABELS.get(occ,"Unknown") if occ >= 0 else "Unknown"
            results[line]["worst_occupancy_int"] = occ

    # ------------------------------------------------------------------ #
    # Problem summary
    # ------------------------------------------------------------------ #

    def _build_problem_summary(self, results):
        for line in self.monitored_lines:
            data = results[line]
            has_alerts = data.get("live_alert_count", data["alert_count"]) > 0
            has_cancellations = data["has_cancellations"]
            has_delays = data["has_delays"]
            max_min = round(data["max_delay_seconds"] / 60)

            # Determine level
            if has_cancellations and (has_alerts or max_min >= 15):
                level = PROBLEM_MAJOR
            elif has_cancellations:
                level = PROBLEM_CANCELLATIONS
            elif has_alerts:
                level = PROBLEM_ALERT
            elif has_delays:
                level = PROBLEM_DELAYS
            else:
                level = PROBLEM_NONE

            data["problem_level"] = level

            # Build TTS-friendly summaries. problem_summary lists overnight
            # works even out of hours (tagged with their window);
            # problem_summary_live covers only what is happening right now.
            disruptions = []
            if has_cancellations:
                n = len(data["cancelled_trips"])
                disruptions.append(f"{n} service{'s' if n!=1 else ''} cancelled")
            if has_delays:
                disruptions.append(
                    f"{data['delay_count']} service{'s' if data['delay_count']!=1 else ''} "
                    f"delayed up to {max_min} minute{'s' if max_min!=1 else ''}"
                )

            def summarise(alert_part: str) -> str:
                parts = ([alert_part] if alert_part else []) + disruptions
                if not parts:
                    return f"{line_phrase(self.mode, line)} is running normally."
                return f"{line_phrase(self.mode, line)}: " + "; ".join(parts) + "."

            data["problem_summary"] = summarise(
                data["alert_text"] if data["alert_count"] > 0 else ""
            )
            data["problem_summary_live"] = summarise(data["live_alert_text"])

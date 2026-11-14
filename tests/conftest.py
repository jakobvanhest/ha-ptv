"""Run the tests in Melbourne time, as Home Assistant would be configured.

The integration takes "local" from Home Assistant's time zone setting, not
the server clock, so the tests pin that setting (restoring it afterwards)
rather than depend on the zone of the machine running them.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

MELBOURNE = ZoneInfo("Australia/Melbourne")

try:
    from homeassistant.util import dt as dt_util
except ImportError:  # gtfs_static tests can run without Home Assistant
    dt_util = None


def local_now() -> datetime:
    return datetime.now(MELBOURNE)


@pytest.fixture(autouse=True)
def melbourne_time_zone():
    if dt_util is None:
        yield
        return
    before = dt_util.DEFAULT_TIME_ZONE
    dt_util.set_default_time_zone(MELBOURNE)
    yield
    dt_util.set_default_time_zone(before)

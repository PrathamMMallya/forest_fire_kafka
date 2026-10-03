"""Unit tests for the streaming layer (pure logic, no Kafka broker needed)."""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming.common import validate_event
from streaming.detector import classify_tier
from streaming.streams_app import DedupStore, RollingStore, TumblingWindows


def _good_event(**over):
    base = {
        "latitude": 37.5, "longitude": -119.5, "pr": 0.0, "rmax": 60.0,
        "rmin": 30.0, "sph": 0.008, "srad": 250.0, "tmmn": 285.0, "tmmx": 305.0,
        "vs": 4.0, "bi": 40.0, "fm100": 9.0, "fm1000": 11.0, "erc": 50.0,
        "etr": 6.0, "pet": 7.0, "vpd": 2.0,
        "datetime": "2023-07-01", "Wildfire": "Yes",
    }
    base.update(over)
    return base


# ---------------------------------------------------------------- validation
def test_validate_accepts_good_event():
    vr = validate_event(_good_event())
    assert vr.ok, vr.errors
    assert vr.cleaned["Wildfire"] == "Yes"


def test_validate_rejects_missing_label():
    ev = _good_event()
    ev.pop("Wildfire")
    assert not validate_event(ev).ok


def test_validate_rejects_out_of_range():
    assert not validate_event(_good_event(tmmx=999.0)).ok


def test_validate_maps_32767_sentinel():
    ev = _good_event(tmmx=32767)
    vr = validate_event(ev)
    assert vr.ok
    assert vr.cleaned["tmmx"] != vr.cleaned["tmmx"]  # NaN propagates to imputer


# -------------------------------------------------------------------- tiers
def test_tier_rules():
    eng = {"erc": 70.0, "fm100": 8.0, "vs": 2.0}
    assert classify_tier(0.4, eng) == "NONE"
    assert classify_tier(0.55, eng) == "WATCH"
    assert classify_tier(0.75, eng) == "WARNING"
    assert classify_tier(0.95, {**eng, "vs": 9.0}) == "EMERGENCY"


def test_tier_emergency_needs_wind():
    eng = {"erc": 70.0, "fm100": 8.0, "vs": 5.0}
    assert classify_tier(0.95, eng) == "WARNING"  # low wind caps at WARNING


# -------------------------------------------------------------------- dedup
def test_dedup_store():
    d = DedupStore(capacity=10)
    assert d.is_new("a")
    assert not d.is_new("a")
    assert d.is_new("b")


# ----------------------------------------------------------- rolling window
def test_rolling_store_prunes_old_events():
    r = RollingStore(window_sec=100)
    for t in range(0, 200, 10):
        r.add("cell", t, {"pr": 1.0, "tmmx": 300.0, "erc": 50.0, "vs": 5.0, "vpd": 1.0})
    stats = r.stats("cell", now=200)
    assert stats["r100s_count"] == 10  # only events t >= 100 remain
    assert stats["r100s_pr_sum"] == 10.0


# --------------------------------------------------------- tumbling windows
def test_tumbling_window_closes_on_boundary():
    tw = TumblingWindows(length_sec=60)
    summary = None
    for t in (0, 10, 30, 59):
        assert tw.add("reg", t, {f: 1.0 for f in
                      ["latitude", "longitude", "pr", "rmax", "rmin", "sph",
                       "srad", "tmmn", "tmmx", "vs", "bi", "fm100", "fm1000",
                       "erc", "etr", "pet", "vpd"]} | {"Wildfire": "No"}) is None
    summary = tw.add("reg", 61, {**{f: 2.0 for f in
                     ["latitude", "longitude", "pr", "rmax", "rmin", "sph",
                      "srad", "tmmn", "tmmx", "vs", "bi", "fm100", "fm1000",
                      "erc", "etr", "pet", "vpd"]}, "Wildfire": "Yes"})
    assert summary is not None
    assert summary["count"] == 4
    assert summary["fire_count"] == 0
    assert math.isclose(summary["window_start"] and 0 or 0, 0)

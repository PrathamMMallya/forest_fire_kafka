"""Unit tests for the India MOSDAC + Open-Meteo streaming layer (pure logic, no broker required)."""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming.common import (
    FEATURES,
    cell_boundary,
    cell_id,
    region_id,
    region_name_for,
    validate_event,
)
from streaming.detector import classify_tier
from streaming.mosdac_parser import estimate_frp, is_in_india
from streaming.streams_app import DedupStore, RollingStore, TumblingWindows, WeatherTableStore


def _good_event(**over):
    base = {
        "latitude": 18.65,
        "longitude": 80.23,
        "t_mir": 311.5,
        "t_tir1": 300.0,
        "delta_t": 11.5,
        "frp_mw": 168.4,
        "solar_elev": 42.0,
        "temp_c": 31.0,
        "rh": 32.0,
        "wind_speed": 4.5,
        "wind_dir": 120.0,
        "precip_mm": 0.0,
        "soil_moisture": 0.14,
        "vpd": 2.2,
        "fwi": 46.0,
        "datetime": "2026-10-24T07:21:00+00:00",
        "Wildfire": "Yes",
    }
    base.update(over)
    return base


# ---------------------------------------------------------------- validation
def test_validate_accepts_good_event():
    vr = validate_event(_good_event())
    assert vr.ok, vr.errors
    assert vr.cleaned["Wildfire"] == "Yes"
    assert vr.cleaned["t_mir"] == 311.5


def test_validate_rejects_missing_label():
    ev = _good_event()
    ev.pop("Wildfire")
    assert not validate_event(ev).ok


def test_validate_rejects_out_of_range():
    assert not validate_event(_good_event(latitude=999.0)).ok
    assert not validate_event(_good_event(t_mir=600.0)).ok


def test_validate_maps_32767_sentinel():
    ev = _good_event(t_mir=32767)
    vr = validate_event(ev)
    assert vr.ok
    assert vr.cleaned["t_mir"] != vr.cleaned["t_mir"]  # NaN propagates to imputer


# ----------------------------------------------------------- FRP & Land Mask
def test_estimate_frp_units():
    # For T_MIR=312K, T_TIR1=301K, 16 km^2 pixel -> ~155.12 MW (no 10^6 unit error or 500 MW clip)
    frp = estimate_frp(312.0, 301.0, pixel_area_km2=16.0)
    assert 150.0 <= frp <= 160.0, f"Expected ~155.12 MW, got {frp}"
    assert estimate_frp(295.0, 298.0) == 0.0


def test_h3_spatial_indexing():
    cid = cell_id(18.65, 80.23)
    rid = region_id(18.65, 80.23)
    poly = cell_boundary(cid)
    rname = region_name_for(18.65, 80.23)

    assert isinstance(cid, str) and len(cid) == 15
    assert isinstance(rid, str) and len(rid) == 15
    assert len(poly) == 6
    assert "Gadchiroli" in rname or "Maharashtra" in rname
    # Verify land mask keeps Tadoba (India) and excludes open Bay of Bengal & Sri Lanka
    assert is_in_india(18.65, 80.23)
    assert not is_in_india(14.0, 86.0)
    assert not is_in_india(6.78, 81.44)


# --------------------------------------------------------- Stream-Table Join
def test_weather_table_join():
    wt = WeatherTableStore()
    cid = cell_id(18.65, 80.23)
    rid = region_id(18.65, 80.23)
    wt.upsert({
        "cell_id": cid,
        "region_id": rid,
        "temp_c": 34.5,
        "rh": 22.0,
        "wind_speed": 5.2,
        "wind_dir": 140.0,
        "precip_mm": 0.0,
        "soil_moisture": 0.11,
        "vpd": 3.4,
        "fwi": 58.2,
    })
    raw_sat = {
        "cell_id": cid,
        "region_id": rid,
        "latitude": 18.65,
        "longitude": 80.23,
        "t_mir": 313.0,
        "t_tir1": 302.0,
        "delta_t": 11.0,
        "frp_mw": 155.0,
        "solar_elev": 44.0,
        "Wildfire": "Yes",
    }
    joined = wt.join(raw_sat)
    assert joined["wind_speed"] == 5.2
    assert joined["fwi"] == 58.2
    assert wt.joins == 1


# -------------------------------------------------------------------- tiers
def test_tier_rules():
    eng = {
        "delta_t": 8.0,
        "t_mir": 305.0,
        "fwi": 26.0,
        "frp_mw": 110.0,
        "wind_speed": 2.2,
        "solar_elev": 40.0,
    }
    assert classify_tier(0.35, eng) == "WATCH"
    assert classify_tier(0.72, {**eng, "delta_t": 9.8, "t_mir": 311.5, "frp_mw": 140.0}) == "WARNING"
    assert (
        classify_tier(
            0.88,
            {**eng, "delta_t": 12.5, "t_mir": 317.0, "wind_speed": 4.2, "frp_mw": 175.0},
        )
        == "EMERGENCY"
    )
    # Sun-glint penalty at solar_elev > 55 deg reduces effective delta_t by 1.5 K
    assert classify_tier(0.35, {**eng, "delta_t": 8.0, "solar_elev": 62.0}) == "NONE"


# -------------------------------------------------------------------- dedup
def test_dedup_store():
    d = DedupStore(capacity=3)
    assert d.is_new("cell_1_ts_1")
    assert not d.is_new("cell_1_ts_1")
    assert d.is_new("cell_2_ts_1")
    assert d.is_new("cell_3_ts_1")
    assert d.is_new("cell_4_ts_1")  # Evicts oldest (cell_2_ts_1 since cell_1 was touched)
    assert len(d.seen) == 3


# ----------------------------------------------------------- rolling window
def test_rolling_store_prunes_old_events():
    r = RollingStore(window_sec=100)
    for t in range(0, 200, 10):
        r.add(
            "cell",
            t,
            {
                "t_mir": 305.0 + t * 0.05,
                "delta_t": 5.0 + t * 0.05,
                "frp_mw": 115.0,
                "wind_speed": 4.0,
                "fwi": 30.0,
                "temp_c": 28.0,
            },
        )
    stats = r.stats("cell", now=200)
    assert stats["r100s_count"] == 10
    assert "r100s_delta_t_max" in stats
    assert stats["delta_t_surge"] > 0


# --------------------------------------------------------- tumbling windows
def test_tumbling_window_closes_on_boundary():
    tw = TumblingWindows(length_sec=60)
    for idx, t in enumerate((0, 15, 30, 50)):
        ev = {f: 1.0 for f in FEATURES}
        # First 2 are fires, next 2 are non-fires, and one feature has NaN
        if idx == 0:
            ev["t_mir"] = float("nan")
        is_fire = idx < 2
        assert tw.add("reg", t, ev, is_fire=is_fire) is None

    next_ev = {f: 2.0 for f in FEATURES}
    summary = tw.add("reg", 65, next_ev, is_fire=True)
    assert summary is not None
    assert summary["count"] == 4
    assert summary["fire_count"] == 2
    assert not math.isnan(summary["mean_t_mir"])

    # Verify periodic watermark flush closes remaining open window
    flushed = tw.flush_expired(current_max_ts=200.0, watermark_sec=30.0)
    assert len(flushed) == 1
    assert flushed[0]["fire_count"] == 1

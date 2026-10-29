"""ISRO MOSDAC Satellite Parser + Open-Meteo Weather Client.

Provides dual-stream data extraction and ingestion for:
1. ISRO INSAT-3DR/3DS Level-2P Active Fire Hotspots (KML) and Level-1C/Smoke rasters (HDF5).
2. Open-Meteo Co-Temporal Hourly & Current Atmospheric Telemetry (wind in m/s) with offline cache.
3. Uber H3 Spatial Indexing (Resolution 6 for cell_id, Resolution 3 for region_id).
"""
from __future__ import annotations

import glob
import json
import math
import os
import re
import xml.etree.ElementTree as ET
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import h3
import requests

# -----------------------------------------------------------------------------
# Indian Forest & Terrains Geo-Reference
# -----------------------------------------------------------------------------

INDIAN_BOUNDS = {
    "lat_min": 6.5,
    "lat_max": 37.5,
    "lon_min": 68.0,
    "lon_max": 97.5,
}

FOREST_REGIONS = [
    ("Satpura / Chhindwara Belt (MP)", 21.5, 23.5, 77.5, 80.5),
    ("Gadchiroli / Tadoba / Pench (Maharashtra)", 18.5, 22.0, 78.5, 81.5),
    ("Bastar / Indravati Forest (Chhattisgarh)", 18.0, 20.5, 80.5, 82.5),
    ("Simlipal / Saranda Belt (Odisha-Jharkhand)", 21.0, 23.0, 83.0, 86.5),
    ("Western Ghats / Bandipur / Nilgiris", 10.5, 13.5, 75.5, 77.5),
    ("Nallamala / Seshachalam Forest (AP/Telangana)", 14.0, 17.5, 78.0, 80.0),
    ("Terai / Dudhwa / Corbett (UP-Uttarakhand)", 28.0, 30.5, 78.5, 81.5),
    ("Northeast Forests (Assam / Nagaland / Mizoram)", 23.5, 28.0, 91.0, 96.5),
    ("Gir Forest / Saurashtra (Gujarat)", 20.8, 22.2, 70.0, 71.5),
    ("Aravalli / Sariska Belt (Rajasthan)", 26.5, 28.5, 75.5, 77.2),
]

# Broader Indian terrestrial sub-regions for representative background sampling
INDIA_LAND_BOXES = FOREST_REGIONS + [
    ("Central Highlands / Vindhya (MP-UP)", 23.5, 26.5, 76.0, 82.5),
    ("Deccan Plateau (Maharashtra-Telangana-Karnataka)", 15.0, 19.5, 74.5, 79.0),
    ("Chota Nagpur & Eastern Plains (Jharkhand-WB-Bihar)", 22.5, 25.5, 84.0, 88.0),
    ("Eastern Ghats / Coastal Andhra-Odisha", 17.5, 21.0, 81.5, 85.5),
    ("Western Plains (Gujarat-Rajasthan-Punjab-Haryana)", 23.0, 30.8, 71.5, 76.5),
    ("Southern Peninsula (Tamil Nadu-Kerala)", 8.5, 13.5, 76.2, 79.8),
    ("Western Himalayas (Himachal-Uttarakhand-J&K)", 29.5, 34.5, 74.5, 80.0),
]


def is_in_india(lat: float, lon: float) -> bool:
    """Checks whether coordinates fall on Indian terrestrial territory (excluding ocean & neighbors)."""
    if not (
        INDIAN_BOUNDS["lat_min"] <= lat <= INDIAN_BOUNDS["lat_max"]
        and INDIAN_BOUNDS["lon_min"] <= lon <= INDIAN_BOUNDS["lon_max"]
    ):
        return False

    # Exclude Sri Lanka
    if lat < 9.9 and lon > 79.4:
        return False

    # Andaman & Nicobar Islands allowance
    is_andaman = 6.7 <= lat <= 13.8 and 92.1 <= lon <= 94.1
    # Lakshadweep Islands allowance
    is_lakshadweep = 8.0 <= lat <= 12.4 and 71.6 <= lon <= 73.8

    if is_andaman or is_lakshadweep:
        return True

    # Exclude Myanmar / Southeast Asia east of Mizoram/Manipur/Nagaland/Arunachal
    if lat < 21.8 and lon > 92.8:
        return False
    if 21.8 <= lat < 24.0 and lon > 93.6:
        return False

    # Exclude Bay of Bengal open water east of the Indian east coast
    east_coast_lon = 80.4 + max(0.0, (lat - 13.0)) * 0.85
    if lat < 21.5 and lon > east_coast_lon and lon < 92.0:
        return False

    # Exclude Arabian Sea open water west of the Indian west coast
    if lat < 20.2:
        west_coast_lon = 72.6 + max(0.0, (20.2 - lat)) * 0.36
        if lon < west_coast_lon:
            return False

    # Exclude Tibetan Plateau north of the Himalayan arc
    if lat > 29.2 and 81.2 < lon < 91.5:
        return False
    if lat > 31.8 and lon > 79.5:
        return False

    # Exclude western Pakistan beyond Rajasthan/Punjab/Gujarat border
    if lat > 24.5 and lon < 69.5:
        return False
    if 28.0 < lat < 32.5 and lon < 73.2:
        return False

    return True


def get_indian_region_name(lat: float, lon: float) -> str:
    """Returns a readable Indian forest reserve or regional name for coordinates."""
    for name, r_lat_min, r_lat_max, r_lon_min, r_lon_max in FOREST_REGIONS:
        if r_lat_min <= lat <= r_lat_max and r_lon_min <= lon <= r_lon_max:
            return name
    for name, r_lat_min, r_lat_max, r_lon_min, r_lon_max in INDIA_LAND_BOXES[len(FOREST_REGIONS):]:
        if r_lat_min <= lat <= r_lat_max and r_lon_min <= lon <= r_lon_max:
            return name
    if is_in_india(lat, lon):
        return f"India ({lat:.2f}N, {lon:.2f}E)"
    return f"South-Asia ({lat:.2f}N, {lon:.2f}E)"


# -----------------------------------------------------------------------------
# Uber H3 Spatial Functions
# -----------------------------------------------------------------------------

def latlon_to_h3(lat: float, lon: float, resolution: int = 6) -> str:
    """Returns the Uber H3 hexagonal cell index for a coordinate.

    Resolution 6: ~3.2 km edge length / ~36.1 km^2 cell area.
    Resolution 3: ~60 km edge length (matches regional tumbling window).
    """
    return h3.latlng_to_cell(lat, lon, resolution)


def h3_to_latlon(h3_index: str) -> Tuple[float, float]:
    """Returns the (lat, lon) centroid for an H3 cell."""
    return h3.cell_to_latlng(h3_index)


def h3_to_boundary(h3_index: str) -> List[List[float]]:
    """Returns the GeoJSON-ready [[lon, lat], ...] polygon boundary."""
    coords = h3.cell_to_boundary(h3_index)
    return [[round(lng, 5), round(lat, 5)] for lat, lng in coords]


# -----------------------------------------------------------------------------
# Solar Elevation & Fire Radiative Power (FRP) Computations
# -----------------------------------------------------------------------------

def calculate_solar_elevation(dt_utc: datetime, lat: float, lon: float) -> float:
    """Calculates solar elevation angle in degrees for sun-glint verification."""
    day_of_year = dt_utc.timetuple().tm_yday
    hour = dt_utc.hour + dt_utc.minute / 60.0 + dt_utc.second / 3600.0

    # Solar declination in radians (Spencer formula approximation)
    gamma = 2.0 * math.pi * (day_of_year - 1) / 365.0
    decl = 0.006918 - 0.399912 * math.cos(gamma) + 0.070257 * math.sin(gamma)

    # Equation of time in minutes
    eqtime = 229.18 * (
        0.000075 + 0.001868 * math.cos(gamma) - 0.032077 * math.sin(gamma)
        - 0.014615 * math.cos(2 * gamma) - 0.040849 * math.sin(2 * gamma)
    )

    # True solar time & hour angle
    time_offset = eqtime + 4.0 * lon
    tst = (hour * 60.0 + time_offset) % 1440.0
    ha_deg = (tst / 4.0) - 180.0
    ha = math.radians(ha_deg)

    lat_rad = math.radians(lat)
    sin_elev = math.sin(lat_rad) * math.sin(decl) + math.cos(lat_rad) * math.cos(decl) * math.cos(ha)
    sin_elev = max(-1.0, min(1.0, sin_elev))
    return round(math.degrees(math.asin(sin_elev)), 2)


def estimate_frp(t_mir: float, t_tir1: float, pixel_area_km2: float = 16.0) -> float:
    """Estimates Fire Radiative Power (MW) from MIR and TIR1 brightness temperatures.

    Uses the Wooster/Kaufman empirical MIR radiance formulation:
        FRP (MW) = 4.34e-19 * A_pixel_km2 * (T_MIR^8 - T_TIR1^8)
    where 4.34e-19 is in MW / (K^8 * km^2) and A_pixel_km2 = 16.0 km^2 (4 km x 4 km INSAT pixel).
    No artificial min/max clamping is applied.
    """
    if t_mir <= t_tir1:
        return 0.0
    coeff = 4.34e-19 * pixel_area_km2
    delta_8 = max(0.0, (t_mir ** 8) - (t_tir1 ** 8))
    frp_mw = coeff * delta_8
    return round(max(0.0, frp_mw), 2)


# -----------------------------------------------------------------------------
# MOSDAC KML Decoder (Fixes ISRO Linear String Accumulation Bug)
# -----------------------------------------------------------------------------

def _parse_scan_dt_from_filename(fname: str) -> datetime:
    """Extracts UTC scan datetime from MOSDAC filename: e.g. 3RIMG_24OCT2026_0715_L2P_FIR_V01R00.kml."""
    scan_match = re.search(r"(\d{2}[A-Z]{3}\d{4})_(\d{4})", fname)
    if scan_match:
        date_str, time_str = scan_match.groups()
        try:
            return datetime.strptime(f"{date_str} {time_str}", "%d%b%Y %H%M").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime(2026, 10, 24, 0, 0, 0, tzinfo=timezone.utc)


def parse_kml_file(kml_path: Path | str, india_only: bool = True) -> List[Dict[str, Any]]:
    """Parses an INSAT-3DR/3DS Level-2P Fire KML file.

    ISRO's MOSDAC KML exporter appends TIR1 and MIR temperature strings into <address>
    without resetting the string buffer, causing the string length to grow linearly across
    placemarks. This parser slices off the previous record's prefix to recover exact Kelvin values.
    """
    kml_path = Path(kml_path)
    ns = {"kml": "http://www.opengis.net/kml/2.2"}

    try:
        tree = ET.parse(kml_path)
    except Exception:
        return []

    fname = kml_path.name
    default_dt = _parse_scan_dt_from_filename(fname)

    placemarks = tree.findall(".//kml:Placemark", ns)
    records = []
    prev_right = ""

    for idx, pm in enumerate(placemarks):
        try:
            addr_elem = pm.find("kml:address", ns)
            if addr_elem is None or not addr_elem.text:
                continue
            addr = addr_elem.text.strip()
            if "," not in addr:
                continue

            left, right = [s.strip() for s in addr.split(",")]
            # Slicing must advance prev_right on every placemark before any geographic filtering
            raw_tir1_str = left[len(prev_right):]
            raw_mir_str = right[len(left):]
            prev_right = right

            if not raw_tir1_str or not raw_mir_str:
                continue

            t_tir1 = float(raw_tir1_str)
            t_mir = float(raw_mir_str)

            # Coordinate extraction
            coord_elem = pm.find(".//kml:coordinates", ns)
            if coord_elem is None or not coord_elem.text:
                continue
            coords = coord_elem.text.strip().split(",")
            lon = float(coords[0])
            lat = float(coords[1])

            if india_only and not is_in_india(lat, lon):
                continue

            # Timestamp extraction
            when_elem = pm.find(".//kml:when", ns)
            if when_elem is not None and when_elem.text:
                when_str = when_elem.text.strip()
                try:
                    dt = datetime.strptime(when_str.replace(" UTC", ""), "%d-%b-%YT%H:%M:%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    dt = default_dt
            else:
                dt = default_dt

            delta_t = round(t_mir - t_tir1, 3)
            frp = estimate_frp(t_mir, t_tir1)
            solar_elev = calculate_solar_elevation(dt, lat, lon)
            h3_c = latlon_to_h3(lat, lon, 6)
            h3_p = latlon_to_h3(lat, lon, 3)
            region_name = get_indian_region_name(lat, lon)

            record = {
                "source": "MOSDAC_INSAT_L2P",
                "file": fname,
                "event_id": f"insat_{h3_c}_{dt.strftime('%Y%m%d%H%M%S')}_{idx}",
                "event_ts": dt.isoformat(),
                "latitude": round(lat, 4),
                "longitude": round(lon, 4),
                "h3_cell": h3_c,
                "h3_region": h3_p,
                "region_name": region_name,
                "t_mir": round(t_mir, 3),
                "t_tir1": round(t_tir1, 3),
                "delta_t": delta_t,
                "frp_mw": frp,
                "solar_elev": solar_elev,
                "Wildfire": "Yes",
            }
            records.append(record)

        except (ValueError, IndexError):
            continue

    return records


def load_all_mosdac_kmls(folder_path: str = "MOSDAC Data/Fire", india_only: bool = True) -> List[Dict[str, Any]]:
    """Loads and chronologically sorts all fire events from MOSDAC KML files."""
    files = sorted(glob.glob(os.path.join(folder_path, "*.kml")))
    all_events = []
    for f in files:
        events = parse_kml_file(f, india_only=india_only)
        all_events.extend(events)
    all_events.sort(key=lambda x: x["event_ts"])
    return all_events


# -----------------------------------------------------------------------------
# MOSDAC HDF5 L1C & Smoke Reader (For .h5 swath/grid files)
# -----------------------------------------------------------------------------

def parse_h5_file(h5_path: Path | str, india_only: bool = True) -> List[Dict[str, Any]]:
    """Reads INSAT-3DS/3DR L1C Geo-projected HDF5 files or smoke products.

    Applies 1D LUT decoding from digital counts (0-1023) to Kelvin and uses
    embedded Latitude/Longitude arrays or HDF5 projection attributes.
    """
    try:
        import h5py
        import numpy as np
    except ImportError:
        return []

    h5_path = Path(h5_path)
    if not h5_path.exists():
        return []

    scan_dt = _parse_scan_dt_from_filename(h5_path.name)
    records = []
    try:
        with h5py.File(h5_path, "r") as f:
            if "IMG_MIR" not in f or "IMG_TIR1" not in f:
                return []

            mir_raw = np.squeeze(f["IMG_MIR"][:])
            tir1_raw = np.squeeze(f["IMG_TIR1"][:])
            lut_mir = f["IMG_MIR_TEMP"][:]
            lut_tir1 = f["IMG_TIR1_TEMP"][:]

            mir_k = lut_mir[np.clip(mir_raw, 0, len(lut_mir) - 1)]
            tir1_k = lut_tir1[np.clip(tir1_raw, 0, len(lut_tir1) - 1)]

            # Extract real geolocation arrays or bounding attributes from HDF5
            lat_grid = f["Latitude"][:] if "Latitude" in f else (f["LAT"][:] if "LAT" in f else None)
            lon_grid = f["Longitude"][:] if "Longitude" in f else (f["LON"][:] if "LON" in f else None)

            lat_min = float(f.attrs.get("lower_latitude", INDIAN_BOUNDS["lat_min"]))
            lat_max = float(f.attrs.get("upper_latitude", INDIAN_BOUNDS["lat_max"]))
            lon_min = float(f.attrs.get("left_longitude", INDIAN_BOUNDS["lon_min"]))
            lon_max = float(f.attrs.get("right_longitude", INDIAN_BOUNDS["lon_max"]))

            valid_mask = (mir_k >= 180.0) & (mir_k <= 400.0) & (tir1_k >= 180.0) & (tir1_k <= 380.0)
            delta_t = mir_k - tir1_k
            hot_mask = valid_mask & (delta_t > 8.0)
            hot_indices = np.argwhere(hot_mask)

            h_rows, w_cols = mir_k.shape
            for r, c in hot_indices:
                t_m = float(mir_k[r, c])
                t_t = float(tir1_k[r, c])
                if lat_grid is not None and lon_grid is not None:
                    lat = float(lat_grid[r, c])
                    lon = float(lon_grid[r, c])
                else:
                    lat = lat_max - (r / max(h_rows - 1, 1)) * (lat_max - lat_min)
                    lon = lon_min + (c / max(w_cols - 1, 1)) * (lon_max - lon_min)

                if india_only and not is_in_india(lat, lon):
                    continue

                h3_c = latlon_to_h3(lat, lon, 6)
                records.append({
                    "source": "MOSDAC_INSAT_HDF5",
                    "file": h5_path.name,
                    "event_id": f"h5_{h3_c}_{scan_dt.strftime('%Y%m%d%H%M')}_{r}_{c}",
                    "event_ts": scan_dt.isoformat(),
                    "latitude": round(lat, 4),
                    "longitude": round(lon, 4),
                    "h3_cell": h3_c,
                    "h3_region": latlon_to_h3(lat, lon, 3),
                    "region_name": get_indian_region_name(lat, lon),
                    "t_mir": round(t_m, 3),
                    "t_tir1": round(t_t, 3),
                    "delta_t": round(t_m - t_t, 3),
                    "frp_mw": estimate_frp(t_m, t_t),
                    "solar_elev": calculate_solar_elevation(scan_dt, lat, lon),
                    "Wildfire": "Yes",
                })
    except Exception:
        pass

    return records


# -----------------------------------------------------------------------------
# Open-Meteo Co-Temporal Weather Client (Wind in m/s)
# -----------------------------------------------------------------------------

def compute_fwi(temp_c: float, rh: float, wind_speed: float, soil_moisture: float, vpd: float) -> float:
    """Computes an empirical Fire Danger / Weather Index (0-100 scale) from meteorological inputs.

    Formula:
        dryness          = max(0, 100 - rh) * 0.35          (0 to 35)
        wind_effect      = min(wind_speed * 3.5, 30.0)      (0 to 30, wind_speed in m/s)
        vpd_effect       = min(vpd * 12.0, 30.0)            (0 to 30, vpd in kPa)
        soil_suppression = max(0, soil_moisture * 50.0)     (0 to 25)
        fwi              = clamp(dryness + wind_effect + vpd_effect - soil_suppression, 0, 100)
    """
    dryness = max(0.0, 100.0 - rh) * 0.35
    wind_effect = min(max(0.0, wind_speed) * 3.5, 30.0)
    vpd_effect = min(max(0.0, vpd) * 12.0, 30.0)
    soil_suppression = max(0.0, soil_moisture * 50.0)
    fwi = dryness + wind_effect + vpd_effect - soil_suppression
    return round(max(0.0, min(100.0, fwi)), 1)


def _safe_float(val: Any, default: float) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


class OpenMeteoWeatherClient:
    """Fetches co-temporal meteorological telemetry from Open-Meteo API with wind_speed_unit=ms."""

    CACHE_FILE = Path("Data/mosdac_weather_cache.json")
    API_URL = "https://api.open-meteo.com/v1/forecast"
    HOURLY_VARS = (
        "temperature_2m,relative_humidity_2m,precipitation,"
        "wind_speed_10m,wind_direction_10m,soil_moisture_0_to_7cm,"
        "vapour_pressure_deficit"
    )

    def __init__(self, cache_file: Path | str | None = None):
        self.cache_file = Path(cache_file or self.CACHE_FILE)
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.cache: Dict[str, Dict[str, Any]] = self._load_cache()

    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        if self.cache_file.exists():
            try:
                raw = json.loads(self.cache_file.read_text(encoding="utf-8"))
                # Retain only genuine Open-Meteo records (discard legacy un-tagged synthetic cache entries)
                return {
                    k: v for k, v in raw.items()
                    if isinstance(v, dict) and v.get("source") == "open_meteo"
                }
            except Exception:
                return {}
        return {}

    def _save_cache(self) -> None:
        try:
            self.cache_file.write_text(json.dumps(self.cache, indent=2), encoding="utf-8")
        except Exception:
            pass

    def get_weather(
        self,
        lat: float,
        lon: float,
        h3_cell: Optional[str] = None,
        scan_dt: Optional[datetime | str] = None,
        use_network: bool = True,
    ) -> Dict[str, float]:
        """Returns meteorological parameters at scan hour (or current), using verified cache or API."""
        cell = h3_cell or latlon_to_h3(lat, lon, 6)
        hour_utc = 12
        if isinstance(scan_dt, str) and scan_dt:
            try:
                hour_utc = datetime.fromisoformat(scan_dt.replace("Z", "+00:00")).hour
            except ValueError:
                hour_utc = 12
        elif isinstance(scan_dt, datetime):
            hour_utc = scan_dt.hour

        hourly_key = f"{cell}_h{hour_utc:02d}"
        if hourly_key in self.cache:
            return dict(self.cache[hourly_key])
        if cell in self.cache:
            # If cell was cached with full 24h series, extract the matching hour
            entry = self.cache[cell]
            if "hourly" in entry and isinstance(entry["hourly"], dict):
                h_str = str(hour_utc)
                if h_str in entry["hourly"]:
                    return dict(entry["hourly"][h_str])
            return dict(entry)

        if use_network:
            try:
                params = {
                    "latitude": round(lat, 3),
                    "longitude": round(lon, 3),
                    "hourly": self.HOURLY_VARS,
                    "wind_speed_unit": "ms",
                    "forecast_days": 1,
                    "timezone": "UTC",
                }
                res = requests.get(self.API_URL, params=params, timeout=5)
                if res.status_code == 200:
                    hourly = res.json().get("hourly", {})
                    self._store_hourly_cell(cell, hourly)
                    if hourly_key in self.cache:
                        self._save_cache()
                        return dict(self.cache[hourly_key])
            except Exception:
                pass

        # Deterministic offline fallback (NOT written to self.cache so cache stays clean)
        return self._fallback_weather(lat, lon, cell, hour_utc)

    def _store_hourly_cell(self, cell: str, hourly: Dict[str, List[Any]]) -> None:
        """Extracts 24-hour diurnal weather profile from Open-Meteo response into cache."""
        times = hourly.get("time", [])
        n = min(len(times), 24)
        if n == 0:
            return
        for h in range(n):
            temp_c = _safe_float((hourly.get("temperature_2m") or [None])[h], 28.0)
            rh = _safe_float((hourly.get("relative_humidity_2m") or [None])[h], 45.0)
            wind_speed = _safe_float((hourly.get("wind_speed_10m") or [None])[h], 2.5)
            wind_dir = _safe_float((hourly.get("wind_direction_10m") or [None])[h], 180.0)
            precip_mm = _safe_float((hourly.get("precipitation") or [None])[h], 0.0)
            soil_moisture = _safe_float((hourly.get("soil_moisture_0_to_7cm") or [None])[h], 0.18)
            vpd = _safe_float((hourly.get("vapour_pressure_deficit") or [None])[h], 1.5)
            fwi = compute_fwi(temp_c, rh, wind_speed, soil_moisture, vpd)

            wx = {
                "temp_c": round(temp_c, 2),
                "rh": round(rh, 1),
                "wind_speed": round(wind_speed, 2),
                "wind_dir": round(wind_dir, 1),
                "precip_mm": round(precip_mm, 2),
                "soil_moisture": round(soil_moisture, 3),
                "vpd": round(vpd, 2),
                "fwi": fwi,
                "source": "open_meteo",
            }
            self.cache[f"{cell}_h{h:02d}"] = wx
            if h == 8:  # 08:00 UTC (~13:30 IST afternoon peak) as cell default
                self.cache[cell] = wx

    def _fallback_weather(self, lat: float, lon: float, cell: str, hour_utc: int = 12) -> Dict[str, float]:
        """Process-independent deterministic fallback using CRC32 (never persisted to cache)."""
        h = zlib.crc32(f"{cell}:{hour_utc}".encode("utf-8")) & 0xFFFFFFFF
        # Convert UTC hour to Indian Standard Time (UTC + 5.5) for realistic diurnal profile
        ist_hour = (hour_utc + 5.5) % 24.0
        diurnal = math.sin(math.pi * max(0.0, ist_hour - 6.0) / 14.0) if 6.0 <= ist_hour <= 20.0 else -0.2

        temp_c = 22.0 + 9.0 * diurnal + ((h % 70) / 10.0) + (28.0 - lat) * 0.22
        rh = max(14.0, min(94.0, 62.0 - 26.0 * diurnal + ((h >> 4) % 25) - 12.0))
        wind_speed = max(0.4, round(1.2 + 1.8 * max(0.0, diurnal) + ((h >> 8) % 45) * 0.1, 2))
        wind_dir = float((h * 17) % 360)
        precip_mm = 0.0 if ((h >> 12) % 14) != 0 else round(((h % 5) + 1) * 0.6, 2)
        soil_moisture = round(0.09 + ((h >> 16) % 22) * 0.01, 3)
        vpd = round(max(0.15, 0.6108 * math.exp(17.27 * temp_c / (temp_c + 237.3)) * (1.0 - rh / 100.0)), 2)
        fwi = compute_fwi(temp_c, rh, wind_speed, soil_moisture, vpd)

        return {
            "temp_c": round(temp_c, 2),
            "rh": round(rh, 1),
            "wind_speed": round(wind_speed, 2),
            "wind_dir": round(wind_dir, 1),
            "precip_mm": round(precip_mm, 2),
            "soil_moisture": round(soil_moisture, 3),
            "vpd": round(vpd, 2),
            "fwi": fwi,
            "source": "fallback",
        }

    def prewarm_cache_for_events(self, events: List[Dict[str, Any]], batch_size: int = 40) -> int:
        """Batch-fetches 24h hourly weather (in m/s) for unique H3 cells from Open-Meteo."""
        unique_cells: Dict[str, Tuple[float, float]] = {}
        for ev in events:
            cell = ev["h3_cell"]
            if f"{cell}_h08" not in self.cache and cell not in unique_cells:
                unique_cells[cell] = (ev["latitude"], ev["longitude"])

        if not unique_cells:
            return 0

        cells = list(unique_cells.keys())
        fetched = 0
        for i in range(0, len(cells), batch_size):
            batch = cells[i: i + batch_size]
            lats = [str(round(unique_cells[c][0], 3)) for c in batch]
            lons = [str(round(unique_cells[c][1], 3)) for c in batch]
            try:
                params = {
                    "latitude": ",".join(lats),
                    "longitude": ",".join(lons),
                    "hourly": self.HOURLY_VARS,
                    "wind_speed_unit": "ms",
                    "forecast_days": 1,
                    "timezone": "UTC",
                }
                res = requests.get(self.API_URL, params=params, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    item_list = data if isinstance(data, list) else [data]
                    for cell, item in zip(batch, item_list):
                        hourly = item.get("hourly", {})
                        self._store_hourly_cell(cell, hourly)
                        fetched += 1
            except Exception:
                pass

        if fetched > 0:
            self._save_cache()
        return fetched

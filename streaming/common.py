"""Shared infrastructure for the India MOSDAC + Open-Meteo wildfire streaming pipeline.

Topics (all created by create_topics.py):
    wildfire.raw         - satellite thermal events, key = Uber H3 cell index (res=6), 6 partitions
    wildfire.weather     - meteorological KTable stream, compacted, key = Uber H3 cell index (res=6)
    wildfire.features    - stream-processor output (joined satellite+weather + window aggregates),
                           key = H3 cell id
    wildfire.predictions - detector output (probability + label + tier), key = H3 cell id
    wildfire.alerts      - tiered emergency alerts with GeoJSON polygons, key = H3 region id (res=3)
    wildfire.dlq         - dead-letter queue for invalid events

Every service reads its Kafka address from KAFKA_BOOTSTRAP (default
localhost:9092) so the same code runs against docker-compose or a local broker.
"""
from __future__ import annotations

import json
import logging
import math
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import h3

from streaming.mosdac_parser import (
    get_indian_region_name,
    h3_to_boundary,
    h3_to_latlon,
    is_in_india,
    latlon_to_h3,
)

# ---------------------------------------------------------------- config ----

def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass
class Settings:
    bootstrap: str = env("KAFKA_BOOTSTRAP", "localhost:9092")
    topic_raw: str = env("TOPIC_RAW", "wildfire.raw")
    topic_weather: str = env("TOPIC_WEATHER", "wildfire.weather")
    topic_features: str = env("TOPIC_FEATURES", "wildfire.features")
    topic_predictions: str = env("TOPIC_PREDICTIONS", "wildfire.predictions")
    topic_alerts: str = env("TOPIC_ALERTS", "wildfire.alerts")
    topic_dlq: str = env("TOPIC_DLQ", "wildfire.dlq")

    # Kafka Streams-style windowing (event time)
    window_tumble_sec: int = env_int("WINDOW_TUMBLE_SEC", 60)     # tumbling window
    window_sliding_sec: int = env_int("WINDOW_SLIDING_SEC", 600)  # sliding window length (10 min)
    window_sliding_hop: int = env_int("WINDOW_SLIDING_HOP", 60)   # slide interval
    watermark_sec: int = env_int("WATERMARK_SEC", 90)             # allowed lateness

    # detector
    alert_gap_sec: int = env_int("ALERT_GAP_SEC", 60)  # per cell+tier dedup gap

    # api
    api_port: int = env_int("API_PORT", 9991)

    # elasticsearch
    es_url: str = env("ES_URL", "http://localhost:9200")


# 15 features: 7 ISRO MOSDAC Satellite Radiometric features + 8 Open-Meteo Weather features
RADIOMETRIC_FEATURES = [
    "latitude", "longitude",
    "t_mir", "t_tir1", "delta_t", "frp_mw", "solar_elev",
]
WEATHER_FEATURES = [
    "temp_c", "rh", "wind_speed", "wind_dir", "precip_mm", "soil_moisture", "vpd", "fwi",
]
FEATURES = RADIOMETRIC_FEATURES + WEATHER_FEATURES

# Plausible ranges used by the validation stage (India territory + physical radiometric/weather bounds)
FEATURE_RANGES = {
    "latitude": (5.0, 39.0),
    "longitude": (65.0, 100.0),
    "t_mir": (180.0, 420.0),          # Mid-Wave Infrared Brightness Temp (Kelvin)
    "t_tir1": (180.0, 380.0),         # Thermal Infrared Brightness Temp (Kelvin)
    "delta_t": (-10.0, 80.0),         # Thermal divergence (Kelvin)
    "frp_mw": (0.0, 1000.0),          # Fire Radiative Power (MW)
    "solar_elev": (-90.0, 90.0),      # Solar elevation angle (degrees)
    "temp_c": (-10.0, 55.0),          # Ambient temperature (deg C)
    "rh": (0.0, 100.0),               # Relative humidity (%)
    "wind_speed": (0.0, 45.0),        # Wind speed (m/s)
    "wind_dir": (0.0, 360.0),         # Wind direction (deg)
    "precip_mm": (0.0, 500.0),        # Precipitation (mm)
    "soil_moisture": (0.0, 1.0),      # Volumetric soil moisture (m3/m3)
    "vpd": (0.0, 15.0),               # Vapor pressure deficit (kPa)
    "fwi": (0.0, 100.0),              # Fire Weather Index (0-100)
}


def cell_id(lat: float, lon: float) -> str:
    """Uber H3 resolution 6 hexagonal index (~3.2 km edge, matches 4 km INSAT pixel)."""
    return latlon_to_h3(lat, lon, 6)


def region_id(lat: float, lon: float) -> str:
    """Uber H3 resolution 3 parent index (~60 km edge) for regional tumbling windows."""
    return latlon_to_h3(lat, lon, 3)


def cell_boundary(h3_index: str) -> List[List[float]]:
    """Returns the [[lon, lat], ...] polygon boundary for GeoJSON rendering."""
    return h3_to_boundary(h3_index)


def region_name_for(lat: float, lon: float) -> str:
    """Returns human-readable Indian forest or state name."""
    return get_indian_region_name(lat, lon)


# ------------------------------------------------------------ validation ----

@dataclass
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    cleaned: dict | None = None


def validate_event(msg: dict) -> ValidationResult:
    """Filter stage: schema + value checks. Returns cleaned event or errors.

    Checks:
      * required keys present
      * numeric parseable, within physical ranges
      * label in {Yes, No}
    """
    errors = []
    if not isinstance(msg, dict):
        return ValidationResult(False, ["payload is not an object"])

    # Support both flat and values-nested payloads
    flat_data = dict(msg)
    if "values" in msg and isinstance(msg["values"], dict):
        flat_data.update(msg["values"])

    missing = [f for f in FEATURES if f not in flat_data]
    if missing:
        errors.append(f"missing fields: {missing}")

    label = flat_data.get("Wildfire")
    if label not in ("Yes", "No"):
        errors.append(f"invalid Wildfire label: {label!r}")

    cleaned: dict = {}
    for f in FEATURES:
        if f not in flat_data:
            continue
        raw = flat_data[f]
        try:
            v = float(raw)
        except (TypeError, ValueError):
            errors.append(f"{f}: not numeric ({raw!r})")
            continue
        if v == 32767.0:  # Sentinel value handling
            v = float("nan")
        if v != v:  # NaN: pass through for imputation
            cleaned[f] = v
            continue
        lo, hi = FEATURE_RANGES[f]
        if not (lo <= v <= hi):
            errors.append(f"{f}: out of range ({v} not in [{lo}, {hi}])")
            continue
        cleaned[f] = v

    if errors:
        return ValidationResult(False, errors)

    cleaned["Wildfire"] = label
    cleaned["datetime"] = flat_data.get("datetime") or flat_data.get("event_ts", "")
    return ValidationResult(True, [], cleaned)


# ----------------------------------------------------------------- serde ----

def to_json(obj) -> bytes:
    return json.dumps(obj, default=_json_default).encode()


def _json_default(o):
    if isinstance(o, float) and o != o:  # NaN
        return None
    if hasattr(o, "isoformat"):
        return o.isoformat()
    raise TypeError(f"unserializable: {type(o)}")


def from_json(data: bytes | str) -> dict:
    return json.loads(data)


# ---------------------------------------------------------------- helpers ---

def now_ms() -> int:
    return int(time.time() * 1000)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
        level=level,
    )
    return logging.getLogger(name)


def log_dir() -> Path:
    d = Path("logs")
    d.mkdir(exist_ok=True)
    return d


def kafka_consumer_conf(group_id: str, settings: Settings | None = None) -> dict:
    s = settings or Settings()
    return {
        "bootstrap.servers": s.bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,  # commit after processing -> at-least-once
        "max.poll.interval.ms": 300000,
    }


def kafka_producer_conf(settings: Settings | None = None) -> dict:
    s = settings or Settings()
    return {
        "bootstrap.servers": s.bootstrap,
        "enable.idempotence": True,  # exactly-once producer side
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
        "message.timeout.ms": 30000,
    }


def produce_safe(producer, topic: str, key: bytes | None, value: bytes) -> None:
    """produce() that survives broker backpressure."""
    while True:
        try:
            producer.produce(topic, key=key, value=value)
            return
        except BufferError:
            producer.poll(0.2)

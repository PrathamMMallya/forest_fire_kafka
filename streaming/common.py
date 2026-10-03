"""Shared infrastructure for the wildfire streaming pipeline.

Topics (all created by create_topics.py):
    wildfire.raw         - producer events, key = grid cell id, 6 partitions
    wildfire.features    - stream-processor output (validated + windowed features),
                           compacted, key = cell id
    wildfire.predictions - detector output (probability + label), key = cell id
    wildfire.alerts      - tiered emergency alerts, key = region id
    wildfire.dlq         - dead-letter queue for invalid events

Every service reads its Kafka address from KAFKA_BOOTSTRAP (default
localhost:9092) so the same code runs against docker-compose or a local broker.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path

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
    topic_features: str = env("TOPIC_FEATURES", "wildfire.features")
    topic_predictions: str = env("TOPIC_PREDICTIONS", "wildfire.predictions")
    topic_alerts: str = env("TOPIC_ALERTS", "wildfire.alerts")
    topic_dlq: str = env("TOPIC_DLQ", "wildfire.dlq")

    # Kafka Streams-style windowing (event time)
    window_tumble_sec: int = env_int("WINDOW_TUMBLE_SEC", 60)     # tumbling window
    window_sliding_sec: int = env_int("WINDOW_SLIDING_SEC", 600)  # sliding window length
    window_sliding_hop: int = env_int("WINDOW_SLIDING_HOP", 60)   # slide interval
    watermark_sec: int = env_int("WATERMARK_SEC", 90)             # allowed lateness

    # detector
    alert_gap_sec: int = env_int("ALERT_GAP_SEC", 60)  # per cell+tier dedup gap

    # api
    api_port: int = env_int("API_PORT", 9991)

    # elasticsearch
    es_url: str = env("ES_URL", "http://localhost:9200")


FEATURES = [
    "latitude", "longitude", "pr", "rmax", "rmin", "sph", "srad",
    "tmmn", "tmmx", "vs", "bi", "fm100", "fm1000", "erc", "etr", "pet", "vpd",
]

# plausible ranges used by the validation stage (gridMET climatology bounds)
FEATURE_RANGES = {
    "latitude": (24.0, 50.0), "longitude": (-125.5, -100.0),
    "pr": (0.0, 400.0), "rmax": (0.0, 100.0), "rmin": (0.0, 100.0),
    "sph": (0.0, 0.03), "srad": (0.0, 500.0),
    "tmmn": (220.0, 320.0), "tmmx": (240.0, 330.0),
    "vs": (0.0, 25.0), "bi": (0.0, 250.0),
    "fm100": (0.5, 45.0), "fm1000": (1.0, 50.0),
    "erc": (0.0, 150.0), "etr": (0.0, 20.0), "pet": (0.0, 20.0),
    "vpd": (0.0, 12.0),
}


def cell_id(lat: float, lon: float) -> str:
    """Quantised grid cell key - partition key so one cell is always ordered."""
    return f"{round(lat, 2):.2f}_{round(lon, 2):.2f}"


def region_id(lat: float, lon: float) -> str:
    """Coarser 1-degree region used for tumbling window aggregates."""
    return f"reg_{int(lat)}_{int(lon)}"


# ------------------------------------------------------------ validation ----

@dataclass
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    cleaned: dict | None = None


def validate_event(msg: dict) -> ValidationResult:
    """Filter stage: schema + value checks. Returns cleaned event or errors.

    Checks (mirrors src/data_prep.py cleaning):
      * required keys present
      * 32767 sentinel values -> NaN -> imputed later by the streams app
      * numeric parseable, within plausible physical ranges
      * label in {Yes, No}
    """
    errors = []
    if not isinstance(msg, dict):
        return ValidationResult(False, ["payload is not an object"])

    missing = [f for f in FEATURES if f not in msg]
    if missing:
        errors.append(f"missing fields: {missing}")

    label = msg.get("Wildfire")
    if label not in ("Yes", "No"):
        errors.append(f"invalid Wildfire label: {label!r}")

    cleaned: dict = {}
    for f in FEATURES:
        if f not in msg:
            continue
        raw = msg[f]
        try:
            v = float(raw)
        except (TypeError, ValueError):
            errors.append(f"{f}: not numeric ({raw!r})")
            continue
        if v == 32767.0:          # gridMET missing-value sentinel
            v = float("nan")
        if v != v:                # NaN: pass through, streams app imputes later
            cleaned[f] = v
            continue
        lo, hi = FEATURE_RANGES[f]
        if not (lo <= v <= hi):
            errors.append(f"{f}: out of range ({v})")
            continue
        cleaned[f] = v

    if errors:
        return ValidationResult(False, errors)

    cleaned["Wildfire"] = label
    cleaned["datetime"] = msg.get("datetime", "")
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
        "enable.auto.commit": False,       # commit after processing -> at-least-once
        "max.poll.interval.ms": 300000,
    }


def kafka_producer_conf(settings: Settings | None = None) -> dict:
    s = settings or Settings()
    return {
        "bootstrap.servers": s.bootstrap,
        "enable.idempotence": True,        # exactly-once producer side
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
        "message.timeout.ms": 30000,
    }


def produce_safe(producer, topic: str, key: bytes | None, value: bytes) -> None:
    """produce() that survives broker backpressure.

    The librdkafka local queue is bounded; when a stage outpaces the broker
    (e.g. replaying a large backlog) produce() raises BufferError. poll()
    serves delivery callbacks and frees queue slots, then we retry - without
    this, a burst kills the service with 'Local: Queue full'.
    """
    while True:
        try:
            producer.produce(topic, key=key, value=value)
            return
        except BufferError:
            producer.poll(0.2)

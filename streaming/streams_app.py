"""The Kafka Streams layer for ISRO MOSDAC + Open-Meteo fire pipeline.

Implements Kafka-Streams semantics directly on the log:
    KTable Join: H3-co-partitioned Stream-Table Join (`wildfire.raw` KStream
                 joined against `wildfire.weather` KTable via WeatherTableStore)
    Filter     : schema + physical range validation, failures -> wildfire.dlq
    Dedup      : event-id O(1) OrderedDict LRU state store (DedupStore)
    Windowing  : event-time tumbling windows (WINDOW_TUMBLE_SEC, per H3 region)
                 with periodic watermark flush + event-time sliding windows
                 (WINDOW_SLIDING_SEC / HOP, per H3 cell)
    Watermark  : events older than max_ts - WATERMARK_SEC flagged as late
    Features   : median imputation, sun-glint flag, diurnal & seasonal cyclic
                 features, rolling thermal & weather aggregates -> wildfire.features

Message types on `wildfire.features`:
    {"type": "cell_features", ...}  enriched per-event feature vector (keyed by H3 cell id)
    {"type": "window_summary", ...} closed tumbling-window summaries (keyed by H3 region id)

Run:  python -m streaming.streams_app
"""
from __future__ import annotations

import math
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone
from pathlib import Path

import joblib
from confluent_kafka import Consumer, Producer

from streaming.common import (
    FEATURES,
    WEATHER_FEATURES,
    Settings,
    ValidationResult,
    cell_boundary,
    cell_id,
    from_json,
    get_logger,
    kafka_consumer_conf,
    kafka_producer_conf,
    now_ms,
    produce_safe,
    region_id,
    region_name_for,
    to_json,
    validate_event,
)

log = get_logger("streams")


class WeatherTableStore:
    """In-memory KTable materialized from `wildfire.weather` (keyed by H3 cell_id & region_id)."""

    def __init__(self):
        self.by_cell: dict[str, dict[str, float]] = {}
        self.by_region: dict[str, dict[str, float]] = {}
        self.updates = 0
        self.joins = 0

    def upsert(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        c_id = payload.get("cell_id")
        r_id = payload.get("region_id")
        wx = {}
        for f in WEATHER_FEATURES:
            if f in payload and payload[f] is not None:
                try:
                    val = float(payload[f])
                    if not math.isnan(val):
                        wx[f] = val
                except (TypeError, ValueError):
                    pass
        if not wx:
            return
        if c_id:
            self.by_cell[c_id] = wx
        if r_id:
            self.by_region[r_id] = wx
        self.updates += 1

    def join(self, raw_event: dict) -> dict:
        """Left-joins a `wildfire.raw` KStream event against the H3 Weather KTable."""
        if not isinstance(raw_event, dict):
            return raw_event
        c_id = raw_event.get("cell_id")
        r_id = raw_event.get("region_id")
        wx = self.by_cell.get(c_id) if c_id else None
        if wx is None and r_id:
            wx = self.by_region.get(r_id)
        if not wx:
            return raw_event

        joined = dict(raw_event)
        joined_any = False
        for f, val in wx.items():
            # Enrich missing/null weather fields or refresh from latest KTable state
            cur = joined.get(f)
            if cur is None or (isinstance(cur, float) and math.isnan(cur)):
                joined[f] = val
                joined_any = True
        if joined_any or c_id in self.by_cell:
            self.joins += 1
        return joined


class DedupStore:
    """O(1) OrderedDict LRU set of seen event ids (Kafka Streams state store analogue)."""

    def __init__(self, capacity: int = 500_000):
        self.capacity = capacity
        self.seen: OrderedDict[str, None] = OrderedDict()

    def is_new(self, event_id: str) -> bool:
        if not event_id:
            return True
        if event_id in self.seen:
            self.seen.move_to_end(event_id)
            return False
        self.seen[event_id] = None
        while len(self.seen) > self.capacity:
            self.seen.popitem(last=False)
        return True


class RollingStore:
    """Per-cell sliding-window aggregates (event time, deque-backed)."""

    def __init__(self, window_sec: int):
        self.window_sec = window_sec
        self.data: dict[str, deque] = {}

    def add(self, key: str, ts: float, values: dict[str, float]) -> None:
        dq = self.data.setdefault(key, deque())
        dq.append((ts, values))
        cutoff = ts - self.window_sec
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def stats(self, key: str, now: float) -> dict:
        dq = self.data.get(key)
        if not dq:
            return {}
        rows = [(t, v) for t, v in dq if t >= now - self.window_sec]
        if not rows:
            return {}
        n = len(rows)
        out: dict[str, float] = {}

        # Rolling means for thermal & weather variables
        for f in ("t_mir", "delta_t", "frp_mw", "wind_speed", "fwi", "temp_c"):
            vals = [v[f] for _, v in rows if f in v and v[f] is not None and not math.isnan(v[f])]
            if vals:
                out[f"r{self.window_sec}s_{f}_mean"] = round(sum(vals) / len(vals), 3)

        # Rolling max for fire surge detection
        for f in ("delta_t", "t_mir", "frp_mw", "wind_speed"):
            vals = [v[f] for _, v in rows if f in v and v[f] is not None and not math.isnan(v[f])]
            if vals:
                out[f"r{self.window_sec}s_{f}_max"] = round(max(vals), 3)

        # Thermal surge rate: difference between latest and earliest delta_t in window
        delta_vals = [v["delta_t"] for _, v in rows if "delta_t" in v and v["delta_t"] is not None and not math.isnan(v["delta_t"])]
        if len(delta_vals) >= 2:
            out["delta_t_surge"] = round(delta_vals[-1] - delta_vals[0], 3)
        else:
            out["delta_t_surge"] = 0.0

        out[f"r{self.window_sec}s_count"] = n
        return out


class TumblingWindows:
    """Per-region event-time tumbling windows -> window_summary emissions."""

    def __init__(self, length_sec: int):
        self.length = length_sec
        self.open: dict[str, dict] = {}
        self.closed = 0

    def add(self, region: str, ts: float, values: dict, is_fire: bool | None = None) -> dict | None:
        start = math.floor(ts / self.length) * self.length
        summary = None
        win = self.open.get(region)
        if win and start > win["start"]:
            summary = self._close(region, win)
        win = self.open.get(region)
        if win is None or win["start"] != start:
            win = {
                "start": start,
                "count": 0,
                "fire": 0,
                "sums": {f: 0.0 for f in FEATURES},
                "valid_counts": {f: 0 for f in FEATURES},
                "max_delta_t": 0.0,
            }
            self.open[region] = win

        if is_fire is None:
            dt_val = values.get("delta_t", 0.0)
            mir_val = values.get("t_mir", 0.0)
            is_fire = (
                values.get("Wildfire") == "Yes"
                or (
                    dt_val is not None
                    and not math.isnan(dt_val)
                    and mir_val is not None
                    and not math.isnan(mir_val)
                    and dt_val >= 8.0
                    and mir_val >= 305.0
                )
            )

        win["count"] += 1
        win["fire"] += 1 if is_fire else 0

        dt_val = values.get("delta_t", 0.0)
        if dt_val is not None and not math.isnan(dt_val) and dt_val > win["max_delta_t"]:
            win["max_delta_t"] = dt_val

        for f in FEATURES:
            v = values.get(f)
            if v is not None and not math.isnan(v):
                win["sums"][f] += v
                win["valid_counts"][f] += 1
        return summary

    def flush_expired(self, current_max_ts: float, watermark_sec: float = 0.0) -> list[dict]:
        """Closes and emits any open windows whose end time has passed the watermark."""
        closed_list: list[dict] = []
        cutoff_start = math.floor((current_max_ts - watermark_sec) / self.length) * self.length
        for region in list(self.open.keys()):
            win = self.open[region]
            if win["start"] < cutoff_start:
                closed_list.append(self._close(region, self.open.pop(region)))
        return closed_list

    def _close(self, region: str, win: dict) -> dict:
        self.closed += 1

        def _mean(feat: str) -> float:
            cnt = max(win.get("valid_counts", {}).get(feat, win["count"]), 1)
            return round(win["sums"][feat] / cnt, 2)

        return {
            "type": "window_summary",
            "region_id": region,
            "window_start": datetime.fromtimestamp(win["start"], tz=timezone.utc).isoformat(),
            "window_sec": self.length,
            "count": win["count"],
            "fire_count": win["fire"],
            "max_delta_t": round(win["max_delta_t"], 2),
            "mean_t_mir": _mean("t_mir"),
            "mean_delta_t": _mean("delta_t"),
            "mean_fwi": _mean("fwi"),
            "mean_wind": _mean("wind_speed"),
            "ts": now_ms(),
        }


class StreamsApp:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or Settings()
        self.consumer = Consumer(kafka_consumer_conf("mosdac-wildfire-streams", self.s))
        self.producer = Producer(kafka_producer_conf(self.s))
        self.consumer.subscribe([self.s.topic_raw, self.s.topic_weather])

        self.weather_table = WeatherTableStore()
        self.dedup = DedupStore()
        self.rolling = RollingStore(self.s.window_sliding_sec)
        self.windows = TumblingWindows(self.s.window_tumble_sec)
        self.max_ts = 0.0
        self.medians: dict[str, float] | None = self._load_saved_medians()
        self._median_buf: list[dict[str, float]] = []

        self.stats = {
            "processed": 0,
            "invalid": 0,
            "duplicates": 0,
            "late": 0,
            "features_emitted": 0,
            "windows_closed": 0,
            "ktable_updates": 0,
            "ktable_joins": 0,
            "started": now_ms(),
        }
        self._last_commit = 0.0
        self._last_stats = 0.0

    @staticmethod
    def _load_saved_medians() -> dict[str, float] | None:
        prep_path = Path("artifacts/preprocessing_artifacts.pkl")
        if prep_path.exists():
            try:
                prep = joblib.load(prep_path)
                meds = prep.get("medians")
                fnames = prep.get("feature_names", FEATURES)
                if meds is not None and len(meds) == len(fnames):
                    return {f: float(m) for f, m in zip(fnames, meds)}
            except Exception:
                pass
        return None

    def _learn_medians(self, values: dict[str, float]) -> None:
        if self.medians is not None:
            return
        clean_row = {f: v for f, v in values.items() if v is not None and not math.isnan(v)}
        if len(clean_row) == len(FEATURES):
            self._median_buf.append(clean_row)
        if len(self._median_buf) >= 200:
            self.medians = {
                f: sorted(b[f] for b in self._median_buf)[len(self._median_buf) // 2]
                for f in FEATURES
            }
            log.info("Imputation medians learned from %d events", len(self._median_buf))
            self._median_buf = []

    def _impute(self, values: dict[str, float]) -> dict[str, float]:
        if self.medians is None:
            self._learn_medians(values)
        out = {}
        for f in FEATURES:
            v = values.get(f)
            if v is None or v != v:
                v = (self.medians or {}).get(f, 0.0)
            out[f] = float(v)
        return out

    def engineer(self, values: dict[str, float], ts: float, event: dict) -> dict:
        raw = dict(values)
        f = self._impute(values)
        cell = event["cell_id"]
        self.rolling.add(cell, ts, f)
        rolling = self.rolling.stats(cell, ts)

        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        doy = dt.timetuple().tm_yday
        hour = dt.hour + dt.minute / 60.0

        solar_elev = f.get("solar_elev", 0.0)
        # High solar elevation (> 55 deg) increases daytime 3.9um solar reflection / sun-glint
        sun_glint_flag = 1.0 if solar_elev > 55.0 else 0.0

        engineered = {
            **f,
            "dry_flag": 1.0 if f.get("precip_mm", 0.0) == 0.0 else 0.0,
            "sun_glint_flag": sun_glint_flag,
            "month_sin": round(math.sin(2 * math.pi * doy / 365.25), 4),
            "month_cos": round(math.cos(2 * math.pi * doy / 365.25), 4),
            "hour_sin": round(math.sin(2 * math.pi * hour / 24.0), 4),
            "hour_cos": round(math.cos(2 * math.pi * hour / 24.0), 4),
            **rolling,
        }
        return {"raw": raw, "imputed": f, "engineered": engineered}

    def to_dlq(self, event: dict | bytes, stage: str, errors: list[str]) -> None:
        payload = {
            "stage": stage,
            "errors": errors,
            "ts": now_ms(),
            "payload": event if isinstance(event, dict) else None,
        }
        produce_safe(self.producer, self.s.topic_dlq, key=b"dlq", value=to_json(payload))
        self.stats["invalid"] += 1

    def process(self, msg) -> None:
        try:
            event = from_json(msg.value())
        except Exception as exc:
            self.to_dlq({"kafka_bytes_error": str(exc)}, "deserialize", [str(exc)])
            return

        # Handle KTable updates from `wildfire.weather`
        topic = msg.topic() if hasattr(msg, "topic") and callable(msg.topic) else self.s.topic_raw
        if topic == self.s.topic_weather or (isinstance(event, dict) and event.get("type") == "weather_update"):
            self.weather_table.upsert(event)
            self.stats["ktable_updates"] = self.weather_table.updates
            return

        # Stream-Table Left Join: enrich `wildfire.raw` event with latest `wildfire.weather` KTable state
        event = self.weather_table.join(event)
        self.stats["ktable_joins"] = self.weather_table.joins

        vr: ValidationResult = validate_event(event)
        if not vr.ok:
            self.to_dlq(event, "validate", vr.errors)
            return

        if not self.dedup.is_new(event.get("event_id", "")):
            self.stats["duplicates"] += 1
            return

        values = {f: vr.cleaned.get(f, float("nan")) for f in FEATURES}
        try:
            ts = datetime.fromisoformat(event["event_ts"]).timestamp()
        except Exception:
            ts = time.time()

        self.max_ts = max(self.max_ts, ts)
        late = ts < self.max_ts - self.s.watermark_sec
        if late:
            self.stats["late"] += 1

        feats = self.engineer(values, ts, event)
        imputed_vals = feats["imputed"]
        lat = imputed_vals["latitude"]
        lon = imputed_vals["longitude"]
        is_fire = vr.cleaned["Wildfire"] == "Yes"

        out = {
            "type": "cell_features",
            "event_id": event["event_id"],
            "cell_id": event["cell_id"],
            "region_id": event.get("region_id") or region_id(lat, lon),
            "region_name": event.get("region_name") or region_name_for(lat, lon),
            "boundary": cell_boundary(event["cell_id"]),
            "event_ts": event["event_ts"],
            "ts": now_ms(),
            "ingest_ts": event.get("ingest_ts"),
            "late": late,
            "label": vr.cleaned["Wildfire"],
            "data_date": event.get("data_date"),
            # `values` holds imputed NaN-safe feature values for downstream model scoring
            "values": imputed_vals,
            "raw_values": feats["raw"],
            "engineered": feats["engineered"],
        }
        produce_safe(
            self.producer,
            self.s.topic_features,
            key=out["cell_id"].encode(),
            value=to_json(out),
        )
        self.stats["features_emitted"] += 1

        summary = self.windows.add(out["region_id"], ts, imputed_vals, is_fire=is_fire)
        if summary:
            produce_safe(
                self.producer,
                self.s.topic_features,
                key=summary["region_id"].encode(),
                value=to_json(summary),
            )
            self.stats["windows_closed"] += 1

        self.stats["processed"] += 1

    def _flush_expired_windows(self) -> None:
        if self.max_ts <= 0:
            return
        for summary in self.windows.flush_expired(self.max_ts, self.s.watermark_sec):
            produce_safe(
                self.producer,
                self.s.topic_features,
                key=summary["region_id"].encode(),
                value=to_json(summary),
            )
            self.stats["windows_closed"] += 1

    def write_stats(self) -> None:
        st = {**self.stats, "uptime_s": (now_ms() - self.stats["started"]) // 1000}
        log_dir_file().write_text(to_json(st).decode())

    def run(self):
        log.info(
            "Streams app online (tumble=%ss slide=%ss/%ss wm=%ss)",
            self.s.window_tumble_sec,
            self.s.window_sliding_sec,
            self.s.window_sliding_hop,
            self.s.watermark_sec,
        )
        try:
            while True:
                msgs = self.consumer.consume(num_messages=200, timeout=0.1)
                for m in msgs:
                    if m.error():
                        log.warning("Consumer error: %s", m.error())
                        continue
                    self.process(m)
                now = time.time()
                if now - self._last_commit > 2:
                    self._flush_expired_windows()
                    self.consumer.commit(asynchronous=True)
                    self._last_commit = now
                if now - self._last_stats > 5:
                    self.write_stats()
                    self._last_stats = now
        except KeyboardInterrupt:
            pass
        finally:
            self._flush_expired_windows()
            self.producer.flush(10)
            self.consumer.close()
            self.write_stats()
            log.info("Streams app stopped: %s", self.stats)


def log_dir_file() -> Path:
    d = Path("logs")
    d.mkdir(exist_ok=True)
    return d / "streams_stats.json"


if __name__ == "__main__":
    StreamsApp().run()

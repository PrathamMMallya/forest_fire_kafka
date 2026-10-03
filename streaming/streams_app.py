"""The "Kafka Streams" layer: filtering & windowing + feature engineering.

Implements Kafka-Streams semantics directly on the Kafka log with a Python
topology (confluent-kafka consumer/producer):

    Filter   : schema + range validation, failures -> wildfire.dlq
    Dedup    : event-id LRU state store
    Windowing: event-time tumbling windows (WINDOW_TUMBLE_SEC, per region)
               + event-time sliding windows (WINDOW_SLIDING_SEC / HOP, per cell)
    Watermark: events older than max_ts - WATERMARK_SEC are flagged late
    Features : median imputation of NaN/sentinels, dry-day flag, calendar
               cyclic features, rolling window aggregates -> wildfire.features

Message types on `wildfire.features`:
    {"type": "cell_features", ...}  enriched per-event feature vector (keyed
                                    by cell id, so topic compaction keeps the
                                    latest state per cell for crash recovery)
    {"type": "window_summary", ...} closed tumbling-window aggregates (keyed
                                    by region id)

Run:  python -m streaming.streams_app
"""
from __future__ import annotations

import math
import time
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from streaming.common import (
    FEATURES, Settings, ValidationResult, cell_id, from_json, get_logger,
    kafka_consumer_conf, kafka_producer_conf, now_ms, produce_safe, region_id,
    to_json, validate_event,
)

log = get_logger("streams")


class DedupStore:
    """LRU set of seen event ids (Kafka Streams table analogue)."""

    def __init__(self, capacity: int = 500_000):
        self.capacity = capacity
        self.seen: dict[str, None] = {}

    def is_new(self, event_id: str) -> bool:
        if event_id in self.seen:
            return False
        self.seen[event_id] = None
        if len(self.seen) > self.capacity:
            for k in list(self.seen)[: self.capacity // 5]:
                self.seen.pop(k, None)
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
        for f in ("tmmx", "erc", "vs", "vpd"):
            vals = [v[f] for _, v in rows if f in v]
            if vals:
                out[f"r{self.window_sec}s_{f}_mean"] = sum(vals) / len(vals)
        for f in ("vs", "erc"):
            vals = [v[f] for _, v in rows if f in v]
            if vals:
                out[f"r{self.window_sec}s_{f}_max"] = max(vals)
        vals = [v["pr"] for _, v in rows if "pr" in v]
        out[f"r{self.window_sec}s_pr_sum"] = sum(vals)
        out[f"r{self.window_sec}s_count"] = n
        return out


class TumblingWindows:
    """Per-region event-time tumbling windows -> window_summary emissions."""

    def __init__(self, length_sec: int):
        self.length = length_sec
        self.open: dict[str, dict] = {}
        self.closed = 0

    def add(self, region: str, ts: float, values: dict) -> dict | None:
        """Feed one event; return a closed window summary when a window ends."""
        start = math.floor(ts / self.length) * self.length
        summary = None
        win = self.open.get(region)
        if win and start > win["start"]:
            summary = self._close(region, win)
        win = self.open.get(region)
        if win is None or win["start"] != start:
            win = {"start": start, "count": 0, "fire": 0,
                   "sums": {f: 0.0 for f in FEATURES}}
            self.open[region] = win
        win["count"] += 1
        win["fire"] += 1 if values.get("Wildfire") == "Yes" else 0
        for f in FEATURES:
            v = values.get(f)
            if v is not None:
                win["sums"][f] += v
        return summary

    def _close(self, region: str, win: dict) -> dict:
        self.closed += 1
        n = max(win["count"], 1)
        return {
            "type": "window_summary",
            "region_id": region,
            "window_start": datetime.fromtimestamp(win["start"], tz=timezone.utc).isoformat(),
            "window_sec": self.length,
            "count": win["count"],
            "fire_count": win["fire"],
            "mean_tmmx": round(win["sums"]["tmmx"] / n, 3),
            "mean_erc": round(win["sums"]["erc"] / n, 3),
            "mean_vs": round(win["sums"]["vs"] / n, 3),
            "sum_pr": round(win["sums"]["pr"], 3),
            "ts": now_ms(),
        }


class StreamsApp:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or Settings()
        self.consumer = Consumer(kafka_consumer_conf("wildfire-streams", self.s))
        self.producer = Producer(kafka_producer_conf(self.s))
        self.consumer.subscribe([self.s.topic_raw])

        self.dedup = DedupStore()
        self.rolling = RollingStore(self.s.window_sliding_sec)
        self.windows = TumblingWindows(self.s.window_tumble_sec)
        self.max_ts = 0.0
        self.medians: dict[str, float] | None = None
        self._median_buf: list[dict[str, float]] = []

        self.stats = {
            "processed": 0, "invalid": 0, "duplicates": 0, "late": 0,
            "features_emitted": 0, "windows_closed": 0, "started": now_ms(),
        }
        self._last_commit = 0
        self._last_stats = 0.0

    # ------------------------------------------------------------- impute ---
    def _learn_medians(self, values: dict[str, float]) -> None:
        if self.medians is not None:
            return
        self._median_buf.append(values)
        if len(self._median_buf) >= 300:
            self.medians = {
                f: sorted(b[f] for b in self._median_buf)[len(self._median_buf) // 2]
                for f in FEATURES
            }
            log.info("imputation medians learned from %d events", len(self._median_buf))
            self._median_buf = []

    def _impute(self, values: dict[str, float]) -> dict[str, float]:
        if self.medians is None:
            self._learn_medians(values)
        out = {}
        for f in FEATURES:
            v = values.get(f)
            if v is None or v != v:  # NaN sentinel from validation
                v = (self.medians or {}).get(f, 0.0)
            out[f] = v
        return out

    # ---------------------------------------------------------- features ----
    def engineer(self, values: dict[str, float], ts: float, event: dict) -> dict:
        raw = dict(values)
        f = self._impute(values)
        cell = event["cell_id"]
        self.rolling.add(cell, ts, f)
        rolling = self.rolling.stats(cell, ts)

        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        doy = dt.timetuple().tm_yday
        engineered = {
            **f,
            "dry_flag": 1.0 if f["pr"] == 0.0 else 0.0,
            "month_sin": round(math.sin(2 * math.pi * doy / 365.25), 4),
            "month_cos": round(math.cos(2 * math.pi * doy / 365.25), 4),
            **rolling,
        }
        return {"raw": raw, "engineered": engineered}

    # --------------------------------------------------------------- dlq ----
    def to_dlq(self, event: dict | bytes, stage: str, errors: list[str]) -> None:
        payload = {
            "stage": stage,
            "errors": errors,
            "ts": now_ms(),
            "payload": event if isinstance(event, dict) else None,
        }
        produce_safe(self.producer, self.s.topic_dlq, key=b"dlq", value=to_json(payload))
        self.stats["invalid"] += 1

    # ------------------------------------------------------------- main -----
    def process(self, msg) -> None:
        try:
            event = from_json(msg.value())
        except Exception as exc:
            self.to_dlq({"kafka_bytes_error": str(exc)}, "deserialize", [str(exc)])
            return

        vr: ValidationResult = validate_event(event)
        if not vr.ok:
            self.to_dlq(event, "validate", vr.errors)
            return

        if not self.dedup.is_new(event.get("event_id", "")):
            self.stats["duplicates"] += 1
            return

        values = {f: vr.cleaned.get(f, float("nan")) for f in FEATURES}
        ts = datetime.fromtimestamp(
            datetime.fromisoformat(event["event_ts"]).timestamp(), tz=timezone.utc
        ).timestamp()
        self.max_ts = max(self.max_ts, ts)
        late = ts < self.max_ts - self.s.watermark_sec
        if late:
            self.stats["late"] += 1

        feats = self.engineer(values, ts, event)
        out = {
            "type": "cell_features",
            "event_id": event["event_id"],
            "cell_id": event["cell_id"],
            "region_id": region_id(values["latitude"], values["longitude"]),
            "event_ts": event["event_ts"],
            "ts": now_ms(),
            "ingest_ts": event.get("ingest_ts"),
            "late": late,
            "label": vr.cleaned["Wildfire"],
            "data_date": event.get("data_date"),
            "values": feats["raw"],
            "engineered": feats["engineered"],
        }
        produce_safe(self.producer, self.s.topic_features,
                     key=out["cell_id"].encode(), value=to_json(out))
        self.stats["features_emitted"] += 1

        summary = self.windows.add(
            region_id(values["latitude"], values["longitude"]), ts, values)
        if summary:
            produce_safe(self.producer, self.s.topic_features,
                         key=summary["region_id"].encode(), value=to_json(summary))
            self.stats["windows_closed"] += 1

        self.stats["processed"] += 1

    def write_stats(self) -> None:
        st = {**self.stats, "uptime_s": (now_ms() - self.stats["started"]) // 1000}
        (log_dir_file()).write_text(to_json(st).decode())

    def run(self):
        log.info("streams app online (tumble=%ss slide=%ss/%ss wm=%ss)",
                 self.s.window_tumble_sec, self.s.window_sliding_sec,
                 self.s.window_sliding_hop, self.s.watermark_sec)
        try:
            while True:
                msgs = self.consumer.consume(num_messages=200, timeout=0.1)
                for m in msgs:
                    if m.error():
                        log.warning("consumer error: %s", m.error())
                        continue
                    self.process(m)
                now = time.time()
                if now - self._last_commit > 2:
                    self.consumer.commit(asynchronous=True)
                    self._last_commit = now
                if now - self._last_stats > 5:
                    self.write_stats()
                    self._last_stats = now
        except KeyboardInterrupt:
            pass
        finally:
            self.producer.flush(10)
            self.consumer.close()
            self.write_stats()
            log.info("streams app stopped: %s", self.stats)


def log_dir_file():
    from pathlib import Path
    d = Path("logs")
    d.mkdir(exist_ok=True)
    return d / "streams_stats.json"


if __name__ == "__main__":
    StreamsApp().run()

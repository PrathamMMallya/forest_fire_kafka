"""ML/DL Fire Detection + Fire Prediction + tiered alerting.

Consumes enriched feature events from `wildfire.features`, scores them with
the trained model artifacts (same contract as inference/model_loader.py:
artifacts/best_RandomForest.pkl + preprocessing_artifacts.pkl), and emits:

    wildfire.predictions  - every scored event (probability, label, model)
    wildfire.alerts       - only tiered alerts (WATCH / WARNING / EMERGENCY)

Alert tiers (from the project methodology):
    WATCH      p > 0.50 and erc > 45
    WARNING    p > 0.70 and erc > 60 and fm100 < 10
    EMERGENCY  p > 0.90 and wind (vs) > 8
Alerts are de-duplicated per (cell, tier) with a configurable gap so a single
burst does not spam the emergency channel.

Run:  python -m streaming.detector
"""
from __future__ import annotations

import time
from pathlib import Path

import joblib
import numpy as np
from confluent_kafka import Consumer, Producer

from streaming.common import (
    Settings, from_json, get_logger, kafka_consumer_conf, kafka_producer_conf,
    now_ms, produce_safe, to_json,
)

log = get_logger("detector")

TIER_RULES = [
    ("EMERGENCY", lambda p, e: p > 0.90 and e.get("vs", 0) > 8),
    ("WARNING",   lambda p, e: p > 0.70 and e.get("erc", 0) > 60 and e.get("fm100", 99) < 10),
    ("WATCH",     lambda p, e: p > 0.50 and e.get("erc", 0) > 45),
]
TIER_RANK = {"NONE": 0, "WATCH": 1, "WARNING": 2, "EMERGENCY": 3}


def classify_tier(prob: float, engineered: dict) -> str:
    for tier, rule in TIER_RULES:
        if rule(prob, engineered):
            return tier
    return "NONE"


class Detector:
    def __init__(self, settings: Settings | None = None, model_path: str | None = None):
        self.s = settings or Settings()
        art = Path(model_path or "artifacts")
        self.model = joblib.load(art / "best_RandomForest.pkl")
        pre = joblib.load(art / "preprocessing_artifacts.pkl")
        self.scaler = pre["scaler"]
        self.feature_names = pre["feature_names"]
        self.model_name = "RandomForest"
        log.info("model loaded: %s (%d features)",
                 self.model_name, len(self.feature_names))

        self.consumer = Consumer(kafka_consumer_conf("wildfire-detector", self.s))
        self.consumer.subscribe([self.s.topic_features])
        self.producer = Producer(kafka_producer_conf(self.s))
        self.last_alert: dict[tuple[str, str], float] = {}
        self.stats = {
            "scored": 0, "alerts": 0, "suppressed": 0,
            "tier_counts": {"NONE": 0, "WATCH": 0, "WARNING": 0, "EMERGENCY": 0},
            "score_ms_total": 0, "started": now_ms(),
        }
        self._last_commit = 0.0
        self._last_stats = 0.0

    def score(self, values: dict) -> tuple[float, int]:
        x = np.array([[float(values[f]) for f in self.feature_names]], dtype="float32")
        x = self.scaler.transform(x)
        prob = float(self.model.predict_proba(x)[0][1])
        return prob, int(prob >= 0.5)

    def score_batch(self, values_list: list[dict]) -> list[float]:
        """Vectorized scoring - one scaler/predict call per consumer batch."""
        if not values_list:
            return []
        x = np.array([[float(v[f]) for f in self.feature_names]
                      for v in values_list], dtype="float32")
        probs = self.model.predict_proba(self.scaler.transform(x))[:, 1]
        return [float(p) for p in probs]

    def maybe_alert(self, event: dict, prob: float, tier: str) -> None:
        if tier == "NONE":
            return
        cell, region = event["cell_id"], event["region_id"]
        gap = self.s.alert_gap_sec
        key = (cell, tier)
        now = time.time()
        if now - self.last_alert.get(key, 0) < gap:
            self.stats["suppressed"] += 1
            return
        self.last_alert[key] = now
        alert = {
            "type": "fire_alert",
            "alert_id": f"{cell}|{tier}|{int(now)}",
            "tier": tier,
            "cell_id": cell,
            "region_id": region,
            "location": {
                "lat": event["values"].get("latitude"),
                "lon": event["values"].get("longitude"),
            },
            "event_ts": event["event_ts"],
            "ts": now_ms(),
            "probability": round(prob, 4),
            "model": self.model_name,
            "trigger": {
                "erc": event["engineered"].get("erc"),
                "fm100": event["engineered"].get("fm100"),
                "wind": event["engineered"].get("vs"),
                "tmmx": event["engineered"].get("tmmx"),
            },
            "message": {
                "WATCH": f"Elevated fire risk at cell {cell} - monitor conditions",
                "WARNING": f"Fire warning for cell {cell} - extreme ERC, low fuel moisture",
                "EMERGENCY": f"EMERGENCY: imminent fire risk at cell {cell} - dispatch",
            }[tier],
        }
        produce_safe(self.producer, self.s.topic_alerts, key=region.encode(),
                     value=to_json(alert))
        self.stats["alerts"] += 1

    def _emit(self, event: dict, prob: float) -> None:
        engineered = event["engineered"]
        engineered.setdefault("vs", engineered.get("r600s_vs_max", 0))
        tier = classify_tier(prob, engineered)
        self.stats["tier_counts"][tier] = self.stats["tier_counts"].get(tier, 0) + 1

        prediction = {
            "type": "prediction",
            "event_id": event["event_id"],
            "cell_id": event["cell_id"],
            "region_id": event["region_id"],
            "location": {
                "lat": event["values"].get("latitude"),
                "lon": event["values"].get("longitude"),
            },
            "event_ts": event["event_ts"],
            "ingest_ts": event.get("ingest_ts"),
            "ts": now_ms(),
            "probability": round(prob, 4),
            "prediction": int(prob >= 0.5),
            "tier": tier,
            "model": self.model_name,
            "label": event.get("label"),
            "late": event.get("late", False),
        }
        produce_safe(self.producer, self.s.topic_predictions,
                     key=event["cell_id"].encode(), value=to_json(prediction))
        self.maybe_alert(event, prob, tier)

    def write_stats(self) -> None:
        from pathlib import Path
        scored = max(self.stats["scored"], 1)
        st = {**self.stats, "avg_score_ms": round(self.stats["score_ms_total"] / scored, 2),
              "uptime_s": (now_ms() - self.stats["started"]) // 1000}
        d = Path("logs")
        d.mkdir(exist_ok=True)
        (d / "detector_stats.json").write_text(to_json(st).decode())

    def run(self):
        log.info("detector online (tiers: WATCH/WARNING/EMERGENCY, gap=%ss)",
                 self.s.alert_gap_sec)
        try:
            while True:
                msgs = self.consumer.consume(num_messages=500, timeout=0.1)
                events = []
                for m in msgs:
                    if m.error():
                        continue
                    try:
                        ev = from_json(m.value())
                    except Exception:
                        continue
                    if ev.get("type") == "cell_features":
                        events.append(ev)  # window summaries are observability-only
                if events:
                    t0 = time.perf_counter()
                    probs = self.score_batch([e["values"] for e in events])
                    self.stats["score_ms_total"] += (time.perf_counter() - t0) * 1000
                    for ev, prob in zip(events, probs):
                        self._emit(ev, prob)
                        self.stats["scored"] += 1
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


if __name__ == "__main__":
    Detector().run()

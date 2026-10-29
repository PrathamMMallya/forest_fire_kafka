"""Real-time wildfire detector for ISRO MOSDAC + Open-Meteo stream.

Consumes enriched feature vectors from `wildfire.features`, runs the
scikit-learn model trained on the 15 MOSDAC+Open-Meteo features, and emits:
  * every prediction -> `wildfire.predictions`
  * tiered alerts (WATCH / WARNING / EMERGENCY) -> `wildfire.alerts`

Alert tiers combine model probability, physical INSAT-3DR/3DS brightness
temperature rules (with daytime sun-glint correction and nighttime sensitivity),
10-minute sliding-window thermal surge (`delta_t_surge`, `r600s_delta_t_max`),
and Open-Meteo Fire Weather Index (FWI).
Per-(cell, tier) alert suppression prevents alert storms.

Run:  python -m streaming.detector
"""
from __future__ import annotations

import math
import time
from pathlib import Path

import joblib
import numpy as np
from confluent_kafka import Consumer, Producer

from streaming.common import (
    FEATURES,
    Settings,
    cell_boundary,
    from_json,
    get_logger,
    kafka_consumer_conf,
    kafka_producer_conf,
    now_ms,
    produce_safe,
    region_name_for,
    to_json,
)

log = get_logger("detector")


def classify_tier(prob: float, e: dict) -> str:
    """Classifies an observation into NONE / WATCH / WARNING / EMERGENCY.

    Incorporates:
    - Model probability `prob`
    - Radiometric anomalies (`delta_t`, `t_mir`, `frp_mw`)
    - Daytime sun-glint penalty (+1.5 K required when `solar_elev > 55°`)
    - Nighttime sensitivity (`solar_elev < 0°` has zero solar reflection)
    - Rolling 10-min window features (`delta_t_surge`, `r600s_delta_t_max`)
    - Meteorological fire spread factors (`wind_speed`, `fwi`, `dry_flag`)
    """
    delta_t = float(e.get("delta_t", 0.0) or 0.0)
    t_mir = float(e.get("t_mir", 0.0) or 0.0)
    frp_mw = float(e.get("frp_mw", 0.0) or 0.0)
    wind = float(e.get("wind_speed", 0.0) or 0.0)
    fwi = float(e.get("fwi", 0.0) or 0.0)
    solar_elev = float(e.get("solar_elev", 0.0) or 0.0)
    surge = float(e.get("delta_t_surge", 0.0) or 0.0)
    roll_max_dt = float(e.get("r600s_delta_t_max", delta_t) or delta_t)

    # Sun-glint correction: require +1.5 K stronger delta_t under zenith sun (> 55 deg)
    glint_penalty = 1.5 if solar_elev > 55.0 else 0.0
    eff_dt = delta_t - glint_penalty

    # Tier 3: EMERGENCY - High-intensity or rapidly spreading active fire
    if prob >= 0.80 and (
        (eff_dt >= 11.2 and wind >= 2.8)
        or frp_mw >= 162.0
        or t_mir >= 315.5
        or (eff_dt >= 10.0 and surge >= 2.5 and fwi >= 32.0)
    ):
        return "EMERGENCY"

    # Tier 2: WARNING - Confirmed contextual fire anomaly
    if (
        (prob >= 0.65 and eff_dt >= 9.0 and t_mir >= 309.0)
        or (prob >= 0.75 and frp_mw >= 125.0)
        or (prob >= 0.70 and roll_max_dt >= 9.5 and fwi >= 30.0)
        or (prob >= 0.65 and solar_elev < 0.0 and delta_t >= 7.5)
    ):
        return "WARNING"

    # Tier 1: WATCH - Developing thermal anomaly or elevated fire-weather risk
    if (
        (prob >= 0.42 and (eff_dt >= 5.8 or fwi >= 25.0))
        or (eff_dt >= 7.5 and t_mir >= 303.0 and fwi >= 22.0)
    ):
        return "WATCH"

    return "NONE"


TIER_RANK = {"NONE": 0, "WATCH": 1, "WARNING": 2, "EMERGENCY": 3}


class ModelBundle:
    """Loads the trained sklearn model + scaler + imputation medians."""

    def __init__(self, model_path: str | None = None):
        art = Path(model_path or "artifacts")
        model_file = art / "best_RandomForest.pkl"
        prep_file = art / "preprocessing_artifacts.pkl"

        if model_file.exists() and prep_file.exists():
            self.model = joblib.load(model_file)
            prep = joblib.load(prep_file)
            self.scaler = prep.get("scaler")
            self.features = list(prep.get("feature_names", FEATURES))
            meds = prep.get("medians")
            self.medians = (
                np.asarray(meds, dtype="float64")
                if meds is not None
                else np.zeros(len(self.features), dtype="float64")
            )
            self.threshold = float(prep.get("threshold", 0.5))
            self.name = "RandomForest_MOSDAC"
            log.info(
                "Model loaded: %s (%d features, threshold=%.2f)",
                self.name,
                len(self.features),
                self.threshold,
            )
        else:
            log.warning("Trained artifacts not found - using physical radiometric fallback scorer")
            self.model = None
            self.scaler = None
            self.features = list(FEATURES)
            self.medians = np.zeros(len(self.features), dtype="float64")
            self.threshold = 0.5
            self.name = "Contextual_INSAT_Fallback"

    def predict_proba(self, rows: list[dict[str, float]]) -> list[float]:
        if self.model is None:
            out = []
            for r in rows:
                dt = r.get("delta_t", 0.0)
                fwi = r.get("fwi", 20.0)
                t_mir = r.get("t_mir", 295.0)
                z = -4.5 + 0.38 * dt + 0.04 * fwi + 0.05 * max(0.0, t_mir - 300.0)
                out.append(round(1.0 / (1.0 + math.exp(-max(-20.0, min(20.0, z)))), 4))
            return out

        X = np.asarray(
            [[r.get(f, float("nan")) for f in self.features] for r in rows],
            dtype="float64",
        )
        # Impute any remaining NaNs using the training medians before scaling
        nan_mask = np.isnan(X)
        if nan_mask.any():
            nan_idx = np.where(nan_mask)
            X[nan_idx[0], nan_idx[1]] = np.take(self.medians, nan_idx[1])

        if self.scaler is not None:
            X = self.scaler.transform(X)
        probs = self.model.predict_proba(X)[:, 1]
        return [float(p) for p in probs]


class Detector:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or Settings()
        self.consumer = Consumer(kafka_consumer_conf("mosdac-wildfire-detector", self.s))
        self.producer = Producer(kafka_producer_conf(self.s))
        self.consumer.subscribe([self.s.topic_features])

        self.bundle = ModelBundle()
        self.model_name = self.bundle.name
        self.last_alert: dict[tuple[str, str], float] = {}

        self.stats = {
            "scored": 0,
            "alerts": 0,
            "suppressed": 0,
            "tier_counts": {"NONE": 0, "WATCH": 0, "WARNING": 0, "EMERGENCY": 0},
            "started": now_ms(),
        }
        self._last_commit = 0.0
        self._last_stats = 0.0

    def score_batch(self, events: list[dict]) -> list[float]:
        # Score on imputed feature values from `engineered` (falling back to `values`)
        return self.bundle.predict_proba([e.get("engineered") or e["values"] for e in events])

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

        eng = event.get("engineered") or event["values"]
        lat = eng.get("latitude")
        lon = eng.get("longitude")
        rname = event.get("region_name") or region_name_for(lat, lon)

        alert = {
            "type": "fire_alert",
            "alert_id": f"{cell}|{tier}|{int(now)}",
            "tier": tier,
            "cell_id": cell,
            "region_id": region,
            "region_name": rname,
            "location": {
                "lat": lat,
                "lon": lon,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [event.get("boundary") or cell_boundary(cell)],
            },
            "event_ts": event["event_ts"],
            "ts": now_ms(),
            "probability": round(prob, 4),
            "model": self.model_name,
            "radiometry": {
                "t_mir": eng.get("t_mir"),
                "t_tir1": eng.get("t_tir1"),
                "delta_t": eng.get("delta_t"),
                "frp_mw": eng.get("frp_mw"),
                "solar_elev": eng.get("solar_elev"),
                "delta_t_surge": eng.get("delta_t_surge", 0.0),
            },
            "weather": {
                "fwi": eng.get("fwi"),
                "temp_c": eng.get("temp_c"),
                "rh": eng.get("rh"),
                "wind_speed": eng.get("wind_speed"),
                "soil_moisture": eng.get("soil_moisture"),
                "vpd": eng.get("vpd"),
            },
            "message": {
                "WATCH": (
                    f"WATCH: Developing thermal anomaly in {rname} [Cell {cell}]. "
                    f"ΔT={eng.get('delta_t')}K, FRP={eng.get('frp_mw')}MW, FWI={eng.get('fwi')}."
                ),
                "WARNING": (
                    f"WARNING: Confirmed Contextual Fire in {rname} [Cell {cell}]. "
                    f"T_MIR={eng.get('t_mir')}K, ΔT={eng.get('delta_t')}K, FRP={eng.get('frp_mw')}MW."
                ),
                "EMERGENCY": (
                    f"EMERGENCY: High-Intensity Spreading Fire in {rname} [Cell {cell}]. "
                    f"FRP={eng.get('frp_mw')}MW, Wind={eng.get('wind_speed')}m/s, ΔT={eng.get('delta_t')}K. Immediate dispatch required!"
                ),
            }[tier],
        }
        produce_safe(
            self.producer,
            self.s.topic_alerts,
            key=region.encode(),
            value=to_json(alert),
        )
        self.stats["alerts"] += 1

    def _emit(self, event: dict, prob: float) -> None:
        engineered = event.get("engineered") or event["values"]
        tier = classify_tier(prob, engineered)
        self.stats["tier_counts"][tier] = self.stats["tier_counts"].get(tier, 0) + 1

        lat = engineered.get("latitude")
        lon = engineered.get("longitude")
        prediction = {
            "type": "prediction",
            "event_id": event["event_id"],
            "cell_id": event["cell_id"],
            "region_id": event["region_id"],
            "region_name": event.get("region_name") or region_name_for(lat, lon),
            "location": {
                "lat": lat,
                "lon": lon,
            },
            "event_ts": event["event_ts"],
            "ingest_ts": event.get("ingest_ts"),
            "ts": now_ms(),
            "probability": round(prob, 4),
            "prediction": int(prob >= self.bundle.threshold),
            "tier": tier,
            "model": self.model_name,
            "label": event.get("label"),
            "late": event.get("late", False),
            "radiometry": {
                "t_mir": engineered.get("t_mir"),
                "t_tir1": engineered.get("t_tir1"),
                "delta_t": engineered.get("delta_t"),
                "frp_mw": engineered.get("frp_mw"),
            },
            "weather": {
                "fwi": engineered.get("fwi"),
                "temp_c": engineered.get("temp_c"),
                "rh": engineered.get("rh"),
                "wind_speed": engineered.get("wind_speed"),
            },
        }
        produce_safe(
            self.producer,
            self.s.topic_predictions,
            key=event["cell_id"].encode(),
            value=to_json(prediction),
        )
        self.maybe_alert(event, prob, tier)
        self.stats["scored"] += 1

    def write_stats(self) -> None:
        d = Path("logs")
        d.mkdir(exist_ok=True)
        st = {
            **self.stats,
            "model": self.model_name,
            "uptime_s": (now_ms() - self.stats["started"]) // 1000,
        }
        (d / "detector_stats.json").write_text(to_json(st).decode())

    def run(self):
        log.info(
            "Detector online (Tiers: WATCH / WARNING / EMERGENCY, gap=%ss)",
            self.s.alert_gap_sec,
        )
        try:
            while True:
                msgs = self.consumer.consume(num_messages=256, timeout=0.1)
                batch = []
                for m in msgs:
                    if m.error():
                        continue
                    try:
                        ev = from_json(m.value())
                    except Exception:
                        continue
                    if ev.get("type") == "cell_features":
                        batch.append(ev)

                if batch:
                    probs = self.score_batch(batch)
                    for ev, p in zip(batch, probs):
                        self._emit(ev, p)

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
            log.info("Detector stopped: %s", self.stats)


if __name__ == "__main__":
    Detector().run()

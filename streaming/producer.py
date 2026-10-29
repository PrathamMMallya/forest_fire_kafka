"""Kafka producer: streams ISRO MOSDAC satellite observations (`wildfire.raw`)
and Open-Meteo meteorological updates (`wildfire.weather`).

Modes
-----
replay   Read the compiled MOSDAC Indian dataset (Data/MOSDAC_Wildfire_Dataset.csv
         or eda/data/eda_sample.csv), emit weather table updates to `wildfire.weather`
         and satellite observations to `wildfire.raw` (both keyed by H3 cell ID).

mosdac   Direct ingestion from raw MOSDAC KML files (MOSDAC Data/Fire/*.kml),
         resolving physical temperatures and attaching co-temporal Open-Meteo weather.

live     Continuous stream anchored to 10 fire-prone Indian forest reserves (Satpura,
         Gadchiroli, Bastar, Simlipal, Bandipur, Corbett, etc.) using the exact same
         physical FRP (`estimate_frp`) and FWI (`compute_fwi`) equations as training.

All events are keyed by Uber H3 resolution 6 cell ID -> co-partitioned for
shuffle-free Stream-Table Join in `streaming/streams_app.py`.
Producer is idempotent (enable.idempotence=True) with acks=all.

Usage:  python -m streaming.producer --mode replay --eps 50 --speed 60
        python -m streaming.producer --mode mosdac --eps 30
        python -m streaming.producer --mode live --eps 10
"""
from __future__ import annotations

import argparse
import math
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

from streaming.common import (
    FEATURES,
    WEATHER_FEATURES,
    Settings,
    cell_id,
    get_logger,
    kafka_producer_conf,
    produce_safe,
    region_id,
    region_name_for,
    to_json,
)
from streaming.mosdac_parser import (
    OpenMeteoWeatherClient,
    calculate_solar_elevation,
    compute_fwi,
    estimate_frp,
    load_all_mosdac_kmls,
)

log = get_logger("producer")

# 10 fire-prone Indian forest reserves (from verified MOSDAC hotspot clusters)
INDIAN_CENTERS = [
    (18.66, 80.23, "Gadchiroli / Tadoba (Maharashtra)"),
    (22.55, 78.79, "Satpura National Park (MP)"),
    (21.57, 79.22, "Pench Forest Belt (Maharashtra-MP)"),
    (21.98, 83.78, "Sundargarh / Simlipal Belt (Odisha)"),
    (22.12, 85.00, "Saranda Forest (Jharkhand)"),
    (28.62, 80.55, "Dudhwa / Pilibhit Terai (UP)"),
    (27.41, 95.04, "Dibru-Saikhowa / Assam Forest"),
    (11.65, 76.62, "Bandipur / Nilgiri Biosphere (Karnataka)"),
    (15.52, 78.85, "Nallamala Forest (Andhra Pradesh)"),
    (19.25, 81.35, "Bastar / Indravati Belt (Chhattisgarh)"),
]


def find_csv() -> Path | None:
    for p in (
        Path("Data/MOSDAC_Wildfire_Dataset.csv"),
        Path("eda/data/eda_sample.csv"),
        Path("Data/Wildfire_Dataset.csv"),
    ):
        if p.exists():
            return p
    return None


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=FEATURES + ["datetime", "Wildfire"])
    df = df.dropna(subset=["datetime"])
    return df


class MosdacReplayProducer:
    def __init__(
        self,
        settings: Settings,
        eps: float,
        speed: float,
        clock: str,
        poison_rate: float,
    ):
        self.s = settings
        self.producer = Producer(kafka_producer_conf(settings))
        self.eps = eps
        self.speed = speed
        self.clock = clock
        self.poison_rate = poison_rate
        self.count = 0
        self.wx_count = 0
        self._last_wx_cell: dict[str, float] = {}
        self.t0 = time.time()
        self._origin = time.time()
        self._rng = random.Random(42)

    def _emit_weather(self, event: dict, key: str):
        """Publishes a compacted KTable weather record to `wildfire.weather` keyed by H3 cell_id."""
        wx_payload = {
            "type": "weather_update",
            "source": "OpenMeteo_API",
            "cell_id": key,
            "region_id": event.get("region_id"),
            "event_ts": event.get("event_ts"),
            **{f: event[f] for f in WEATHER_FEATURES if f in event},
        }
        produce_safe(
            self.producer,
            self.s.topic_weather,
            key=key.encode(),
            value=to_json(wx_payload),
        )
        self.wx_count += 1

    def _emit(self, event: dict, key: str):
        # Emit weather table update if cell is new or periodically refreshed
        now = time.time()
        if key not in self._last_wx_cell or (now - self._last_wx_cell[key] > 30.0):
            if all(isinstance(event.get(f), (int, float)) for f in WEATHER_FEATURES):
                self._emit_weather(event, key)
                self._last_wx_cell[key] = now

        produce_safe(
            self.producer,
            self.s.topic_raw,
            key=key.encode(),
            value=to_json(event),
        )
        self.count += 1
        if self.count % 1000 == 0:
            el = time.time() - self.t0
            log.info(
                "Produced %d raw events & %d weather KTable updates (%.1f eps avg)",
                self.count,
                self.wx_count,
                self.count / max(el, 0.01),
            )

    def _poison(self, base_event: dict) -> dict:
        """Inject a malformed event to exercise validation + DLQ."""
        bad = dict(base_event)
        field = self._rng.choice(FEATURES)
        bad[field] = self._rng.choice(["corrupted_data", 99999.0, None, 32767.5])
        if self._rng.random() < 0.3:
            bad.pop("Wildfire", None)
        return bad

    def event_envelope(self, row: dict, seq: int, loop_epoch: int = 0) -> dict:
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        cell = cell_id(lat, lon)
        reg = region_id(lat, lon)

        if self.clock == "data":
            try:
                ts = datetime.fromisoformat(str(row["datetime"])).replace(tzinfo=timezone.utc)
            except ValueError:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.fromtimestamp(
                self._origin + seq / max(self.speed, 1), tz=timezone.utc
            )

        # Deterministic observation event_id within each replay epoch so duplicate packets are caught
        obs_sig = f"{cell}|{row['datetime']}|{float(row['t_mir']):.2f}|{float(row['t_tir1']):.2f}"
        event_id = obs_sig if loop_epoch == 0 else f"{obs_sig}|epoch{loop_epoch}"

        event = {
            "type": "satellite_observation",
            "source": "ISRO_MOSDAC_INSAT",
            "event_id": event_id,
            "cell_id": cell,
            "region_id": reg,
            "region_name": region_name_for(lat, lon),
            "event_ts": ts.isoformat(),
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
            "seq": seq,
            **{f: float(row[f]) for f in FEATURES},
            "Wildfire": row["Wildfire"],
            "data_date": str(row["datetime"]),
        }
        return event

    def run_replay(self, df: pd.DataFrame, limit: int, loop: bool):
        log.info(
            "Replaying %d rows from CSV (clock=%s, eps=%.1f, speed=%.0fx)",
            len(df),
            self.clock,
            self.eps,
            self.speed,
        )
        seq = 0
        loop_epoch = 0
        next_t = time.perf_counter()
        while True:
            for _, row in df.iterrows():
                if limit and seq >= limit:
                    self._flush()
                    log.info("Replay finished: %d events", seq)
                    return
                event = self.event_envelope(row.to_dict(), seq, loop_epoch=loop_epoch)
                if self.poison_rate and self._rng.random() < self.poison_rate:
                    event = self._poison(event)
                self._emit(event, event["cell_id"])
                seq += 1
                next_t += 1.0 / max(self.eps, 0.1)
                delay = next_t - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                if self.count % 500 == 0:
                    self.producer.poll(0)
            if not loop:
                self._flush()
                log.info("Replay finished: %d events", seq)
                return
            loop_epoch += 1
            log.info("Dataset exhausted - looping (epoch=%d)", loop_epoch)

    def run_mosdac_kml(self, folder: str, limit: int, loop: bool):
        """Direct stream from parsed MOSDAC KML files with co-temporal Open-Meteo weather."""
        log.info("Loading MOSDAC KML files from %s...", folder)
        events = load_all_mosdac_kmls(folder, india_only=True)
        if not events:
            raise SystemExit(f"No Indian fire events found in {folder}")

        log.info("Connecting Open-Meteo weather client (wind_speed in m/s)...")
        wx_client = OpenMeteoWeatherClient()
        wx_client.prewarm_cache_for_events(events)

        rows = []
        for ev in events:
            lat = ev["latitude"]
            lon = ev["longitude"]
            h3_c = ev["h3_cell"]
            wx = wx_client.get_weather(lat, lon, h3_cell=h3_c, scan_dt=ev["event_ts"], use_network=False)
            rows.append({
                **ev,
                **wx,
                "datetime": ev["event_ts"],
            })

        df = pd.DataFrame(rows)
        log.info("Loaded %d MOSDAC fire events with co-temporal weather.", len(df))
        self.run_replay(df, limit, loop)

    def run_live(self, eps: float):
        """Continuous live feed anchored to 10 Indian forest reserves using unified FRP & FWI physics."""
        log.info("Starting live Indian forest feed at %.1f eps...", eps)
        wx_client = OpenMeteoWeatherClient()

        # Initialize per-reserve state from Open-Meteo cache/API
        state = {}
        for center in INDIAN_CENTERS:
            lat_c, lon_c, _ = center
            wx = wx_client.get_weather(lat_c, lon_c, use_network=False)
            state[center] = {
                "temp_c": wx["temp_c"],
                "rh": wx["rh"],
                "wind": wx["wind_speed"],
                "wind_dir": wx["wind_dir"],
                "soil": wx["soil_moisture"],
                "fire_steps_remaining": 0,
                "fire_intensity": 0.0,
            }

        seq = 0
        next_t = time.perf_counter()
        while True:
            for center in INDIAN_CENTERS:
                lat_c, lon_c, _ = center
                st = state[center]

                now_utc = datetime.now(timezone.utc)
                solar_elev = calculate_solar_elevation(now_utc, lat_c, lon_c)
                is_day = solar_elev > 0

                # Evolve 2m air temperature & meteorology smoothly
                st["temp_c"] = max(14.0, min(44.0, st["temp_c"] + self._rng.gauss(0, 0.25)))
                st["rh"] = max(14.0, min(94.0, st["rh"] + self._rng.gauss(0, 0.8)))
                st["wind"] = max(0.4, min(18.0, st["wind"] + self._rng.gauss(0, 0.25)))
                st["wind_dir"] = (st["wind_dir"] + self._rng.gauss(0, 4.0)) % 360.0
                st["soil"] = max(0.05, min(0.42, st["soil"] + self._rng.gauss(0, 0.003)))

                temp_c = round(st["temp_c"], 2)
                rh = round(st["rh"], 1)
                wind_speed = round(st["wind"], 2)
                soil_moisture = round(st["soil"], 3)
                vpd = round(
                    max(0.15, 0.6108 * math.exp(17.27 * temp_c / (temp_c + 237.3)) * (1.0 - rh / 100.0)),
                    2,
                )
                # Unified FWI formula (identical to training and mosdac_parser)
                fwi = compute_fwi(temp_c, rh, wind_speed, soil_moisture, vpd)

                # Land surface brightness temperature (t_tir1 in K) = air temp + solar skin heating
                skin_heating = (3.5 + 4.5 * math.sin(math.radians(max(0.0, solar_elev)))) if is_day else -1.5
                t_tir1 = round(temp_c + 273.15 + skin_heating + self._rng.gauss(0, 0.6), 3)

                # Fire episode state machine (sustained multi-step fire episodes when FWI is conducive)
                if st["fire_steps_remaining"] > 0:
                    st["fire_steps_remaining"] -= 1
                    is_fire = True
                    # Intensity ramps up then decays
                    st["fire_intensity"] = max(7.2, min(16.5, st["fire_intensity"] + self._rng.gauss(0.4, 1.1)))
                    delta_t = round(st["fire_intensity"], 3)
                    wildfire_label = "Yes"
                elif self._rng.random() < (0.06 if fwi > 30.0 else 0.02):
                    st["fire_steps_remaining"] = self._rng.randint(3, 7)
                    st["fire_intensity"] = self._rng.uniform(7.5, 12.5)
                    is_fire = True
                    delta_t = round(st["fire_intensity"], 3)
                    wildfire_label = "Yes"
                else:
                    is_fire = False
                    delta_t = round(max(-1.5, self._rng.gauss(1.8 if is_day else 0.5, 1.1)), 3)
                    wildfire_label = "No"

                t_mir = round(t_tir1 + delta_t, 3)
                # Unified physical FRP formula (identical to training and mosdac_parser)
                frp = estimate_frp(t_mir, t_tir1)

                lat = round(lat_c + self._rng.gauss(0, 0.04), 4)
                lon = round(lon_c + self._rng.gauss(0, 0.04), 4)

                row = {
                    "latitude": lat,
                    "longitude": lon,
                    "t_mir": t_mir,
                    "t_tir1": t_tir1,
                    "delta_t": delta_t,
                    "frp_mw": frp,
                    "solar_elev": solar_elev,
                    "temp_c": temp_c,
                    "rh": rh,
                    "wind_speed": wind_speed,
                    "wind_dir": round(st["wind_dir"], 1),
                    "precip_mm": 0.0,
                    "soil_moisture": soil_moisture,
                    "vpd": vpd,
                    "fwi": fwi,
                    "datetime": now_utc.isoformat(),
                    "Wildfire": wildfire_label,
                }

                event = self.event_envelope(row, seq, loop_epoch=seq)
                self._emit(event, event["cell_id"])
                seq += 1

                next_t += 1.0 / max(eps, 0.1)
                delay = next_t - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                if self.count % 500 == 0:
                    self.producer.poll(0)

    def _flush(self):
        self.producer.flush(30)
        log.info("Flushed producer queue.")


def main():
    ap = argparse.ArgumentParser(description="MOSDAC + Open-Meteo Kafka Producer")
    ap.add_argument(
        "--mode",
        choices=["replay", "mosdac", "live"],
        default="replay",
        help="replay CSV | mosdac raw KMLs | live synthetic stream",
    )
    ap.add_argument("--csv", default=None, help="override input dataset CSV path")
    ap.add_argument("--kml-dir", default="MOSDAC Data/Fire", help="directory with MOSDAC KML files")
    ap.add_argument("--eps", type=float, default=50.0, help="events per second")
    ap.add_argument("--speed", type=float, default=60.0, help="synthetic stream-time compression factor")
    ap.add_argument("--clock", choices=["synthetic", "data"], default="synthetic")
    ap.add_argument("--limit", type=int, default=0, help="0 = full dataset / loop")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument(
        "--poison-rate",
        type=float,
        default=0.005,
        help="fraction of malformed events to exercise DLQ",
    )
    args = ap.parse_args()

    s = Settings()
    rp = MosdacReplayProducer(s, args.eps, args.speed, args.clock, args.poison_rate)

    if args.mode == "live":
        rp.run_live(args.eps)
        return
    elif args.mode == "mosdac":
        rp.run_mosdac_kml(args.kml_dir, args.limit, args.loop)
        return

    path = Path(args.csv) if args.csv else find_csv()
    if path is None:
        raise SystemExit("No dataset found - run `python -m eda.generate_sample` first.")
    df = load_csv(path)
    rp.run_replay(df, args.limit, args.loop)


if __name__ == "__main__":
    main()

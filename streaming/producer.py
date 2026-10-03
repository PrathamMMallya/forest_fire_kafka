"""Kafka producer: streams the fire-weather dataset into `wildfire.raw`.

Modes
-----
replay   Read a CSV (Data/Wildfire_Dataset.csv or the EDA sample) row by row
         and produce each record as one grid-cell-day event. The replay can
         run on *synthetic stream time* (default: event_ts advances with the
         wall clock, so windows behave in a short demo) or on *data time*
         (event_ts = the recorded date, for backfill style replays).

live     Endless mode: synthesize new cell-day events from a random walk over
        the 7 fire-prone regions (seeded), so the dashboard never starves.

Events are keyed by grid cell id -> land on the same partition -> per-cell
ordering. The producer is idempotent (enable.idempotence) with acks=all.

Usage:  python -m streaming.producer --mode replay --eps 50 --speed 60
        python -m streaming.producer --mode live --eps 20
"""
from __future__ import annotations

import argparse
import math
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

from streaming.common import (
    Settings, cell_id, get_logger, kafka_producer_conf, now_ms, produce_safe,
    to_json,
)

log = get_logger("producer")

# fire-prone region centres (from the EDA spatial analysis)
CENTERS = [
    (37.5, -119.5), (41.3, -122.8), (39.8, -105.5), (46.8, -113.9),
    (34.3, -111.8), (40.5, -116.8), (43.5, -110.5),
]

FEATURES = [
    "latitude", "longitude", "pr", "rmax", "rmin", "sph", "srad",
    "tmmn", "tmmx", "vs", "bi", "fm100", "fm1000", "erc", "etr", "pet", "vpd",
]


def find_csv() -> Path | None:
    for p in (Path("Data/Wildfire_Dataset.csv"), Path("eda/data/eda_sample.csv")):
        if p.exists():
            return p
    return None


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=FEATURES + ["datetime", "Wildfire"])
    df = df.dropna(subset=["datetime"])
    return df


class ReplayProducer:
    def __init__(self, settings: Settings, eps: float, speed: float,
                 clock: str, poison_rate: float):
        self.s = settings
        self.producer = Producer(kafka_producer_conf(settings))
        self.eps = eps
        self.speed = speed            # data-time multiplier for replay clock
        self.clock = clock            # "synthetic" | "data"
        self.poison_rate = poison_rate
        self.count = 0
        self.t0 = time.time()
        self._origin = time.time()    # synthetic stream-time origin
        self._rng = random.Random(7)

    # ------------------------------------------------------------------ #
    def _emit(self, event: dict, key: str):
        produce_safe(self.producer, self.s.topic_raw,
                     key=key.encode(), value=to_json(event))
        self.count += 1
        if self.count % 2000 == 0:
            el = time.time() - self.t0
            log.info("produced %d events (%.0f eps avg)", self.count, self.count / el)

    def _poison(self, base_event: dict) -> dict:
        """Inject a malformed event to exercise validation + DLQ."""
        bad = dict(base_event)
        field = self._rng.choice(FEATURES)
        bad[field] = self._rng.choice(["not_a_number", 99999.0, None, 32767.5])
        if self._rng.random() < 0.3:
            bad.pop("Wildfire", None)
        return bad

    # ------------------------------------------------------------------ #
    def event_envelope(self, row: dict, seq: int) -> dict:
        cell = cell_id(row["latitude"], row["longitude"])
        if self.clock == "data":
            try:
                ts = datetime.fromisoformat(str(row["datetime"])).replace(
                    tzinfo=timezone.utc)
            except ValueError:
                ts = datetime.now(timezone.utc)
        else:
            # synthetic stream time: compress the replay so windows close
            # within seconds (1 event slot = 1 second of stream time / speed)
            ts = datetime.fromtimestamp(
                self._origin + seq / max(self.speed, 1), tz=timezone.utc)
        event = {
            "event_id": f"{cell}|{row['datetime']}|{seq}",
            "cell_id": cell,
            "event_ts": ts.isoformat(),
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
            "seq": seq,
            **{f: float(row[f]) for f in FEATURES},
            "Wildfire": row["Wildfire"],
            "data_date": str(row["datetime"]),
        }
        return event

    def run_replay(self, df: pd.DataFrame, limit: int, loop: bool):
        log.info("replaying %d rows from CSV (clock=%s, eps=%.0f, speed=%.0fx)",
                 len(df), self.clock, self.eps, self.speed)
        seq = 0
        next_t = time.perf_counter()
        while True:
            for _, row in df.iterrows():
                if limit and seq >= limit:
                    self._flush()
                    log.info("replay finished: %d events", seq)
                    return
                event = self.event_envelope(row.to_dict(), seq)
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
                log.info("replay finished: %d events", seq)
                return
            log.info("CSV exhausted - looping (fresh event ids)")

    # ------------------------------------------------------------------ #
    def run_live(self, eps: float):
        log.info("live synthetic feed at %.0f eps", eps)
        state = {c: {"tmmx": 295.0, "rh": 40.0, "wind": 3.0, "pr": 0.0,
                     "fm100": 12.0, "erc": 40.0, "vpd": 1.2}
                 for c in CENTERS}
        seq = 0
        next_t = time.perf_counter()
        while True:
            for centre in CENTERS:
                st = state[centre]
                month = (datetime.now().timetuple().tm_yday / 365) * 2 * math.pi
                season = 0.5 * (1 - math.cos(month))
                st["tmmx"] += self._rng.gauss(0, 0.4) + 0.01 * (season * 305 - st["tmmx"])
                st["rh"] = max(5, min(95, st["rh"] + self._rng.gauss(0, 1.5)
                                     - 0.5 * (season - 0.5)))
                st["wind"] = max(0.3, min(18, st["wind"] + self._rng.gauss(0, 0.4)))
                st["pr"] = 0.0 if self._rng.random() > 0.06 + 0.1 * (1 - season) \
                    else round(self._rng.expovariate(0.4), 2)
                st["fm100"] = max(2, min(35, st["fm100"] + self._rng.gauss(0, 0.5)
                                         - 0.15 * (st["tmmx"] - 295)))
                st["erc"] = max(0, min(120, st["erc"] + self._rng.gauss(0, 1.2)
                                       + 0.2 * (st["tmmx"] - 295) - 0.3 * st["pr"]))
                st["vpd"] = max(0, min(8, 0.25 * (st["tmmx"] - 273)
                                       * (1 - st["rh"] / 100)))
                lat, lon = centre
                lat += self._rng.gauss(0, 0.6)
                lon += self._rng.gauss(0, 0.7)
                row = {
                    "latitude": round(lat, 4), "longitude": round(lon, 4),
                    "pr": st["pr"], "rmax": min(100, st["rh"] + 15),
                    "rmin": st["rh"], "sph": 0.002 + 0.010 * st["rh"] / 50,
                    "srad": 150 + 120 * season, "tmmn": st["tmmx"] - 12,
                    "tmmx": round(st["tmmx"], 2), "vs": round(st["wind"], 2),
                    "bi": round(0.9 * st["erc"] * (0.6 + 0.06 * st["wind"]), 1),
                    "fm100": round(st["fm100"], 2),
                    "fm1000": round(st["fm100"] + 1.5, 2),
                    "erc": round(st["erc"], 1),
                    "etr": round(4 + 3 * season, 2),
                    "pet": round(5 + 3.5 * season, 2),
                    "vpd": round(st["vpd"], 3),
                    "datetime": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "Wildfire": "Yes" if self._rng.random() < 0.04 * (0.3 + season)
                                * (1 + st["erc"] / 100) else "No",
                }
                event = self.event_envelope(row, seq)
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
        log.info("flushed producer queue")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["replay", "live"], default="replay")
    ap.add_argument("--csv", default=None, help="override input CSV path")
    ap.add_argument("--eps", type=float, default=50.0, help="events per second")
    ap.add_argument("--speed", type=float, default=60.0,
                    help="synthetic stream-time compression factor")
    ap.add_argument("--clock", choices=["synthetic", "data"], default="synthetic")
    ap.add_argument("--limit", type=int, default=0, help="0 = full CSV / loop")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--poison-rate", type=float, default=0.005,
                    help="fraction of malformed events (exercise the DLQ)")
    args = ap.parse_args()

    s = Settings()
    rp = ReplayProducer(s, args.eps, args.speed, args.clock, args.poison_rate)
    if args.mode == "live":
        rp.run_live(args.eps)
        return
    path = Path(args.csv) if args.csv else find_csv()
    if path is None:
        raise SystemExit("no CSV found - run eda/generate_sample.py first")
    df = load_csv(path)
    rp.run_replay(df, args.limit, args.loop)


if __name__ == "__main__":
    main()

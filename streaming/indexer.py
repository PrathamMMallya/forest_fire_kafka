"""Index fire alerts into Elasticsearch (Kafka -> ES stage of the pipeline).

Consumes `wildfire.alerts` and bulk-indexes each alert into the
`wildfire-alerts` index (searchable from Kibana / GET /api/search).

Uses the ES HTTP API directly (no extra client dependency). If ES is down the
service retries with backoff and keeps an in-memory buffer, so alerts are not
lost while ES restarts.

Run:  python -m streaming.indexer
"""
from __future__ import annotations

import time

import requests
from confluent_kafka import Consumer

from streaming.common import (
    Settings,
    from_json,
    get_logger,
    kafka_consumer_conf,
    now_ms,
    to_json,
)

log = get_logger("es-indexer")
INDEX = "wildfire-alerts"
MAPPING = {
    "mappings": {
        "properties": {
            "tier": {"type": "keyword"},
            "cell_id": {"type": "keyword"},
            "region_id": {"type": "keyword"},
            "region_name": {"type": "keyword"},
            "location": {"type": "geo_point"},
            "probability": {"type": "float"},
            "model": {"type": "keyword"},
            "message": {"type": "text"},
            "event_ts": {"type": "date", "format": "strict_date_optional_time"},
            "@timestamp": {"type": "date"},
            "radiometry": {
                "properties": {
                    "t_mir": {"type": "float"},
                    "t_tir1": {"type": "float"},
                    "delta_t": {"type": "float"},
                    "frp_mw": {"type": "float"},
                    "solar_elev": {"type": "float"},
                }
            },
            "weather": {
                "properties": {
                    "fwi": {"type": "float"},
                    "temp_c": {"type": "float"},
                    "rh": {"type": "float"},
                    "wind_speed": {"type": "float"},
                    "soil_moisture": {"type": "float"},
                    "vpd": {"type": "float"},
                }
            },
        }
    }
}


class Indexer:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or Settings()
        self.consumer = Consumer(kafka_consumer_conf("mosdac-wildfire-indexer", self.s))
        self.consumer.subscribe([self.s.topic_alerts])
        self.buffer: list[dict] = []
        self.indexed = 0
        self.es_ok = False
        self.stats = {"indexed": 0, "buffered": 0, "es_up": False, "started": now_ms()}

    def ensure_index(self) -> bool:
        try:
            r = requests.head(f"{self.s.es_url}/{INDEX}", timeout=3)
            if r.status_code == 200:
                return True
            r = requests.put(f"{self.s.es_url}/{INDEX}", json=MAPPING, timeout=5)
            r.raise_for_status()
            log.info("Created Elasticsearch index %s", INDEX)
            return True
        except requests.RequestException as exc:
            log.warning("ES unavailable: %s", exc)
            return False

    def flush(self, force: bool = False) -> None:
        if not self.buffer:
            return
        if len(self.buffer) < 50 and not force:
            return
        lines = []
        for alert in self.buffer:
            doc = {**alert, "@timestamp": alert.get("ts")}
            lines.append('{"index": {"_index": "%s"}}' % INDEX)
            lines.append(to_json(doc).decode())
        body = "\n".join(lines) + "\n"
        try:
            r = requests.post(
                f"{self.s.es_url}/_bulk",
                data=body,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=10,
            )
            r.raise_for_status()
            self.indexed += len(self.buffer)
            self.stats["indexed"] = self.indexed
            self.buffer.clear()
            self.es_ok = True
        except requests.RequestException as exc:
            log.warning(
                "Bulk index failed (%s) - %d alerts buffered", exc, len(self.buffer)
            )
            self.es_ok = False

    def run(self):
        log.info("Indexer online -> %s/%s", self.s.es_url, INDEX)
        last_retry = 0.0
        last_stats = 0.0
        try:
            while True:
                msgs = self.consumer.consume(num_messages=200, timeout=0.5)
                for m in msgs:
                    if m.error():
                        continue
                    try:
                        self.buffer.append(from_json(m.value()))
                    except Exception:
                        continue
                if not self.es_ok and time.time() - last_retry > 10:
                    self.es_ok = self.ensure_index()
                    last_retry = time.time()
                self.flush()
                if time.time() - last_stats > 10:
                    self.stats["buffered"] = len(self.buffer)
                    self.stats["es_up"] = self.es_ok
                    self.stats["uptime_s"] = (now_ms() - self.stats["started"]) // 1000
                    last_stats = time.time()
        except KeyboardInterrupt:
            pass
        finally:
            self.flush(force=True)
            self.consumer.close()
            log.info("Indexer stopped: %d indexed total", self.indexed)


if __name__ == "__main__":
    Indexer().run()

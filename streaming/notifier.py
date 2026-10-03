"""Emergency alert dispatch (last pipeline stage).

Consumes `wildfire.alerts` and dispatches:
  * console dispatch log (always)
  * POST to an emergency webhook if EMERGENCY_WEBHOOK_URL is set
  * WARNING/EMERGENCY tiers are treated as page-worthy, WATCH is log-only

Run:  python -m streaming.notifier
"""
from __future__ import annotations

import os
import time

import requests
from confluent_kafka import Consumer

from streaming.common import Settings, from_json, get_logger, kafka_consumer_conf, now_ms

log = get_logger("notifier")
WEBHOOK = os.environ.get("EMERGENCY_WEBHOOK_URL", "")
PAGE_TIERS = {"WARNING", "EMERGENCY"}


class Notifier:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or Settings()
        self.consumer = Consumer(kafka_consumer_conf("wildfire-notifier", self.s))
        self.consumer.subscribe([self.s.topic_alerts])
        self.stats = {"received": 0, "paged": 0, "webhook_ok": 0,
                      "webhook_fail": 0, "started": now_ms()}

    def dispatch(self, alert: dict) -> None:
        tier = alert.get("tier", "WATCH")
        line = (f"[{tier:^9}] cell={alert.get('cell_id')} "
                f"p={alert.get('probability')} "
                f"erc={alert.get('trigger', {}).get('erc')} "
                f"| {alert.get('message')}")
        (log.info if tier == "WATCH" else log.warning)(line)

        if tier in PAGE_TIERS and WEBHOOK:
            try:
                requests.post(WEBHOOK, json=alert, timeout=5)
                self.stats["webhook_ok"] += 1
            except requests.RequestException as exc:
                log.error("webhook failed: %s", exc)
                self.stats["webhook_fail"] += 1

    def run(self):
        log.info("notifier online (webhook=%s)", "configured" if WEBHOOK else "console-only")
        last_stats = 0.0
        try:
            while True:
                msgs = self.consumer.consume(num_messages=200, timeout=0.5)
                for m in msgs:
                    if m.error():
                        continue
                    try:
                        self.dispatch(from_json(m.value()))
                        self.stats["received"] += 1
                        if from_json(m.value()).get("tier") in PAGE_TIERS:
                            self.stats["paged"] += 1
                    except Exception:
                        continue
                if time.time() - last_stats > 10:
                    self.stats["uptime_s"] = (now_ms() - self.stats["started"]) // 1000
                    last_stats = time.time()
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    Notifier().run()

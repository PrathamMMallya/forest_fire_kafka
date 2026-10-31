"""Audit the key -> partition mapping on a Kafka topic.

Every event in this pipeline is keyed by `cell_id` (streaming/common.py:81) so
that all events for one grid cell land on the same partition. That is what
guarantees per-cell ordering, which in turn is what makes RollingStore's 600s
sliding averages (streaming/streams_app.py:58) meaningful. If a cell's events
were ever spread over two partitions, those averages would be computed over an
interleaved, out-of-order timeline -- and nothing would raise. The numbers would
just be subtly wrong.

This script samples messages from every partition and checks that invariant,
then reports how evenly the keys are spread.

Run:
    .venv/bin/python scripts/inspect_partitions.py
    .venv/bin/python scripts/inspect_partitions.py --topic wildfire.features -n 500
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from streaming.common import Settings, get_logger, kafka_consumer_conf  # noqa: E402

log = get_logger("partition-audit")


def sample_partitions(topic: str, per_partition: int,
                      settings: Settings) -> list[tuple[int, str]]:
    """Read `per_partition` messages from the END of each partition.

    Returns a list of (partition, key) pairs.
    """
    conf = kafka_consumer_conf(f"wildfire-audit-{topic}", settings)
    conf["group.id"] = f"wildfire-audit-{topic}"
    c = Consumer(conf)

    md = c.list_topics(topic, timeout=10)
    if topic not in md.topics:
        raise SystemExit(f"topic {topic!r} does not exist")
    parts = sorted(md.topics[topic].partitions)
    log.info("topic %s has %d partitions: %s", topic, len(parts), parts)

    out: list[tuple[int, str]] = []
    for p in parts:
        tp = TopicPartition(topic, p)
        _, high = c.get_watermark_offsets(tp, timeout=10)
        start = max(0, high - per_partition)
        c.assign([TopicPartition(topic, p, start)])
        got = 0
        while got < per_partition:
            msg = c.poll(2.0)
            if msg is None:
                break
            if msg.error():
                continue
            key = msg.key()
            out.append((p, key.decode() if key else "<null>"))
            got += 1
        log.info("partition %d: sampled %d messages (offsets %d..%d)", p, got, start, high)
    c.close()
    return out


def audit(samples: list[tuple[int, str]]) -> dict:
    """Check that every key appears on exactly one partition.

    TODO: implement this. `samples` is [(partition, key), ...] collected from
    every partition of the topic.

    Build a map key -> set of partitions it was seen on, then decide what to
    report back. Return a dict with at least:
        "keys_seen"     distinct keys observed
        "violations"    keys that appeared on more than one partition
        "per_partition" {partition: distinct key count}
        "balanced"      bool - is the key spread even across partitions?

    Two judgement calls worth thinking through:
      * A key seen on two partitions is a real violation and should be loud.
        A key seen on one is fine even if it was sampled only once - a cell-day
        appears in the CSV once, so single observations are expected.
      * "Even" is a range, not a point. Sampling finite windows from each
        partition means counts will never match exactly. Pick a tolerance and
        justify it - 5%? 10%? What would you consider evidence of a hot key?
    """
    raise NotImplementedError("implement audit() - see the TODO above")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="wildfire.raw")
    ap.add_argument("-n", "--messages", type=int, default=300,
                    help="messages to sample per partition")
    args = ap.parse_args()

    settings = Settings()
    samples = sample_partitions(args.topic, args.messages, settings)
    log.info("collected %d samples", len(samples))

    report = audit(samples)
    for k, v in report.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
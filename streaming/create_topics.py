"""Create the wildfire Kafka topics (idempotent - safe to re-run)."""
from confluent_kafka.admin import AdminClient, NewTopic

from streaming.common import Settings, get_logger

log = get_logger("create-topics")


def ensure_topics(settings: Settings | None = None) -> None:
    s = settings or Settings()
    admin = AdminClient({"bootstrap.servers": s.bootstrap})

    spec = [
        # (name, partitions, replication, config)
        (s.topic_raw, 6, 3, {"retention.ms": str(7 * 24 * 3600 * 1000)}),
        (s.topic_features, 3, 3, {"cleanup.policy": "compact",
                                  "min.cleanable.dirty.ratio": "0.1"}),
        (s.topic_predictions, 3, 3, {"retention.ms": str(24 * 3600 * 1000)}),
        (s.topic_alerts, 3, 3, {"retention.ms": str(90 * 24 * 3600 * 1000)}),
        (s.topic_dlq, 1, 3, {"retention.ms": str(30 * 24 * 3600 * 1000)}),
    ]
    # single-broker dev cluster: replication 1 when only one broker
    try:
        meta = admin.list_topics(timeout=10)
        brokers = len(meta.brokers)
    except Exception:
        brokers = 1
    topics = [
        NewTopic(name, num_partitions=p, replication_factor=min(rf, brokers),
                 config=cfg)
        for name, p, rf, cfg in spec
    ]

    existing = set(admin.list_topics(timeout=10).topics)
    new = [t for t in topics if t.topic not in existing]
    if not new:
        log.info("all topics already exist")
        return

    for name, fut in admin.create_topics(new).items():
        try:
            fut.result(timeout=30)
            log.info("created %s", name)
        except Exception as exc:
            log.warning("topic %s: %s", name, exc)


if __name__ == "__main__":
    ensure_topics()

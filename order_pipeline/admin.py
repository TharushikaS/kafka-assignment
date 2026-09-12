"""Topic provisioning.

Auto-topic-creation is disabled on the broker (a production best practice), so
the ``orders`` and ``orders.DLQ`` topics are created explicitly and idempotently
here. Run as a module before the first producer/consumer start::

    python -m order_pipeline.admin
"""

from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic

from .config import Config
from .logging_config import configure_logging

logger = logging.getLogger(__name__)


def create_topics(config: Config) -> None:
    """Create the orders and DLQ topics if they do not already exist."""
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics.keys())
    wanted = [config.orders_topic, config.dlq_topic]
    to_create = [
        NewTopic(
            name,
            num_partitions=config.topic_partitions,
            replication_factor=config.topic_replication_factor,
        )
        for name in wanted
        if name not in existing
    ]

    if not to_create:
        logger.info("Topics already present: %s", ", ".join(wanted))
        return

    futures = admin.create_topics(to_create)
    for name, future in futures.items():
        try:
            future.result()  # Block until the operation completes.
            logger.info("Created topic %r", name)
        except Exception as exc:  # noqa: BLE001 - report and continue for others.
            logger.error("Failed to create topic %r: %s", name, exc)
            raise


def main() -> None:
    configure_logging()
    config = Config.from_env()
    logger.info("Provisioning topics on %s ...", config.bootstrap_servers)
    create_topics(config)
    logger.info("Done.")


if __name__ == "__main__":
    main()

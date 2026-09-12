"""Centralised, environment-driven configuration.

All tunables live in one place so the producer, consumer and tests share a
single source of truth. Values are read from environment variables (optionally
populated from a local ``.env`` file) and fall back to defaults that match the
bundled ``docker-compose.yml`` stack, so the system runs with zero configuration
out of the box.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

try:  # Load a local .env file if python-dotenv is installed. Optional.
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover - dotenv is a convenience, not a requirement.
    pass


@dataclass(frozen=True)
class Config:
    """Immutable snapshot of the pipeline configuration.

    Use :meth:`from_env` to construct an instance from environment variables;
    the defaults below apply when a variable is unset.
    """

    # --- Connectivity ---
    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"

    # --- Topics ---
    orders_topic: str = "orders"
    dlq_topic: str = "orders.DLQ"
    topic_partitions: int = 3
    topic_replication_factor: int = 1

    # --- Consumer ---
    consumer_group: str = "order-processors"

    # --- Retry policy (transient failures) ---
    max_retries: int = 3
    retry_base_delay: float = 0.5
    retry_backoff_factor: float = 2.0
    retry_max_delay: float = 8.0
    retry_jitter: float = 0.3

    # --- Failure injection / business rules ---
    transient_failure_rate: float = 0.15
    max_price: float = 10_000.0

    @classmethod
    def from_env(cls) -> "Config":
        """Build a :class:`Config`, reading each field from the environment."""
        env = os.environ
        return cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            schema_registry_url=env.get("SCHEMA_REGISTRY_URL", cls.schema_registry_url),
            orders_topic=env.get("ORDERS_TOPIC", cls.orders_topic),
            dlq_topic=env.get("DLQ_TOPIC", cls.dlq_topic),
            topic_partitions=int(env.get("TOPIC_PARTITIONS", cls.topic_partitions)),
            topic_replication_factor=int(
                env.get("TOPIC_REPLICATION_FACTOR", cls.topic_replication_factor)
            ),
            consumer_group=env.get("CONSUMER_GROUP", cls.consumer_group),
            max_retries=int(env.get("MAX_RETRIES", cls.max_retries)),
            retry_base_delay=float(env.get("RETRY_BASE_DELAY", cls.retry_base_delay)),
            retry_backoff_factor=float(
                env.get("RETRY_BACKOFF_FACTOR", cls.retry_backoff_factor)
            ),
            retry_max_delay=float(env.get("RETRY_MAX_DELAY", cls.retry_max_delay)),
            retry_jitter=float(env.get("RETRY_JITTER", cls.retry_jitter)),
            transient_failure_rate=float(
                env.get("TRANSIENT_FAILURE_RATE", cls.transient_failure_rate)
            ),
            max_price=float(env.get("MAX_PRICE", cls.max_price)),
        )

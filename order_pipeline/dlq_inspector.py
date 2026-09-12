"""Read and pretty-print the contents of the Dead Letter Queue.

A small operational tool for the live demo: it drains the DLQ topic from the
beginning, decoding each original order and printing the diagnostic headers that
explain why it failed.

Usage::

    python -m order_pipeline.dlq_inspector
    python -m order_pipeline.dlq_inspector --timeout 5
"""

from __future__ import annotations

import argparse
import logging
import uuid

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import Config
from .logging_config import configure_logging
from .models import dict_to_order
from .schemas import load_order_schema

logger = logging.getLogger(__name__)


def inspect_dlq(config: Config, idle_timeout: float = 5.0) -> int:
    """Print every record currently in the DLQ; return the count seen."""
    sr_client = SchemaRegistryClient({"url": config.schema_registry_url})
    deserializer = AvroDeserializer(sr_client, load_order_schema(), dict_to_order)

    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            # A throwaway group so we always read from the beginning.
            "group.id": f"dlq-inspector-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config.dlq_topic])

    seen = 0
    print(f"\n=== Dead Letter Queue: {config.dlq_topic} ===")
    try:
        while True:
            msg = consumer.poll(idle_timeout)
            if msg is None:
                break  # No more messages within the idle window.
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("DLQ read error: %s", msg.error())
                continue

            seen += 1
            headers = {k: v.decode("utf-8") for k, v in (msg.headers() or [])}
            try:
                order = deserializer(
                    msg.value(), SerializationContext(config.dlq_topic, MessageField.VALUE)
                )
                order_desc = (
                    f"orderId={order.orderId} product={order.product} price={order.price}"
                )
            except Exception as exc:  # noqa: BLE001 - undecodable payload.
                order_desc = f"<undecodable: {exc}>"

            print(f"\n[{seen}] {order_desc}")
            print(f"     reason : {headers.get('x-error-type')}: {headers.get('x-error-message')}")
            print(
                f"     origin : {headers.get('x-original-topic')}"
                f"[{headers.get('x-original-partition')}]"
                f"@{headers.get('x-original-offset')}"
                f"  attempts={headers.get('x-attempts')}"
                f"  failed_at={headers.get('x-failed-at')}"
            )
    finally:
        consumer.close()

    print(f"\n=== {seen} message(s) in DLQ ===\n")
    return seen


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect the Dead Letter Queue.")
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="seconds to wait for a new record before concluding the DLQ is drained",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = parse_args(argv)
    inspect_dlq(Config.from_env(), idle_timeout=args.timeout)


if __name__ == "__main__":
    main()

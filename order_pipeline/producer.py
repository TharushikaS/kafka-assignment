"""Order producer.

Generates random ``Order`` messages, serializes them with Avro against the
Schema Registry, and publishes them to the orders topic keyed by ``orderId``
(so all events for one order land on the same partition and preserve ordering).

A configurable fraction of *invalid* orders (non-positive price) is emitted on
purpose so the consumer's permanent-failure -> DLQ path can be demonstrated.

Usage::

    python -m order_pipeline.producer --count 200 --invalid-rate 0.05 --rate 20
"""

from __future__ import annotations

import argparse
import logging
import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from .config import Config
from .logging_config import configure_logging
from .models import Order, order_to_dict
from .schemas import load_order_schema

logger = logging.getLogger(__name__)

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def build_serializer(config: Config) -> AvroSerializer:
    """Create an Avro value serializer wired to the Schema Registry."""
    sr_client = SchemaRegistryClient({"url": config.schema_registry_url})
    return AvroSerializer(sr_client, load_order_schema(), order_to_dict)


def generate_order(order_id: int, invalid_rate: float, rng: random.Random) -> Order:
    """Build one random order; occasionally an invalid one (negative price)."""
    product = rng.choice(PRODUCTS)
    if rng.random() < invalid_rate:
        # Invalid order: a negative price -> permanent validation failure -> DLQ.
        price = round(rng.uniform(-50.0, -1.0), 2)
    else:
        price = round(rng.uniform(1.0, 500.0), 2)
    return Order(orderId=str(1000 + order_id), product=product, price=price)


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Delivery failed for key %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered to %s [%d] @ offset %d", msg.topic(), msg.partition(), msg.offset()
        )


def produce_orders(
    config: Config,
    count: int,
    invalid_rate: float,
    rate: float,
    seed: int | None = None,
) -> None:
    """Produce ``count`` orders at approximately ``rate`` messages/second."""
    rng = random.Random(seed)
    key_serializer = StringSerializer("utf_8")
    value_serializer = build_serializer(config)
    producer = Producer({"bootstrap.servers": config.bootstrap_servers})

    topic = config.orders_topic
    interval = 1.0 / rate if rate > 0 else 0.0

    logger.info("Producing %d orders to %r (invalid_rate=%.2f) ...", count, topic, invalid_rate)
    for i in range(count):
        order = generate_order(i, invalid_rate, rng)
        producer.produce(
            topic=topic,
            key=key_serializer(order.orderId, SerializationContext(topic, MessageField.KEY)),
            value=value_serializer(
                order, SerializationContext(topic, MessageField.VALUE)
            ),
            on_delivery=_delivery_report,
        )
        producer.poll(0)  # Trigger delivery callbacks.
        logger.info("Produced %s: %s @ %.2f", order.orderId, order.product, order.price)
        if interval:
            time.sleep(interval)

    remaining = producer.flush(timeout=30)
    if remaining:
        logger.warning("%d message(s) still in queue after flush timeout", remaining)
    logger.info("Producer finished.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce random Avro order messages.")
    parser.add_argument("--count", type=int, default=100, help="number of orders to send")
    parser.add_argument(
        "--invalid-rate",
        type=float,
        default=0.05,
        help="fraction of orders emitted with an invalid (negative) price",
    )
    parser.add_argument(
        "--rate", type=float, default=10.0, help="approximate messages per second (0 = as fast as possible)"
    )
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for reproducibility")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = parse_args(argv)
    produce_orders(
        Config.from_env(),
        count=args.count,
        invalid_rate=args.invalid_rate,
        rate=args.rate,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()

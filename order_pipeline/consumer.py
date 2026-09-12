"""Order consumer.

Consumes Avro-encoded ``Order`` messages and, for each one:

1. deserializes it against the Schema Registry,
2. processes it with retry-on-transient-failure (exponential backoff),
3. on success, folds its price into the real-time running average, and
4. on permanent failure (or exhausted retries), routes it to the DLQ.

Offsets are committed **manually, only after** a message has been either
processed or routed to the DLQ. This gives at-least-once delivery with no
silent message loss: a crash mid-processing simply re-delivers the message.

Usage::

    python -m order_pipeline.consumer
    python -m order_pipeline.consumer --max-messages 200   # stop after N (for demos/tests)
"""

from __future__ import annotations

import argparse
import logging
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .aggregator import PriceAggregator
from .config import Config
from .dlq import send_to_dlq
from .errors import PermanentError, ProcessingError, RetriesExhausted
from .logging_config import configure_logging
from .models import dict_to_order
from .processing import process_order
from .retry import run_with_retry
from .schemas import load_order_schema

logger = logging.getLogger(__name__)


class OrderConsumer:
    """Encapsulates the consume loop and its collaborators."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.aggregator = PriceAggregator()
        self._running = True

        sr_client = SchemaRegistryClient({"url": config.schema_registry_url})
        self._deserializer = AvroDeserializer(sr_client, load_order_schema(), dict_to_order)

        self._consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,  # We commit explicitly after handling.
            }
        )
        # A plain byte producer for the DLQ (original payload is forwarded as-is).
        self._dlq_producer = Producer({"bootstrap.servers": config.bootstrap_servers})

    def stop(self, *_: Any) -> None:
        """Request a graceful shutdown (used as a signal handler)."""
        logger.info("Shutdown requested; finishing current message ...")
        self._running = False

    def _handle_message(self, msg: Message) -> None:
        """Deserialize, process (with retry), aggregate, or route to DLQ."""
        topic = msg.topic()
        try:
            order = self._deserializer(
                msg.value(), SerializationContext(topic, MessageField.VALUE)
            )
        except Exception as exc:  # noqa: BLE001 - a corrupt/undecodable payload.
            # Cannot even decode it: this is permanently broken -> DLQ.
            send_to_dlq(
                self._dlq_producer,
                self.config.dlq_topic,
                msg,
                error_type="DeserializationError",
                error_message=str(exc),
                attempts=0,
            )
            return

        try:
            run_with_retry(
                lambda: process_order(order, self.config),
                self.config,
            )
        except PermanentError as exc:
            send_to_dlq(
                self._dlq_producer,
                self.config.dlq_topic,
                msg,
                error_type="PermanentError",
                error_message=str(exc),
                attempts=1,
            )
            return
        except RetriesExhausted as exc:
            send_to_dlq(
                self._dlq_producer,
                self.config.dlq_topic,
                msg,
                error_type="RetriesExhausted",
                error_message=str(exc),
                attempts=exc.attempts,
            )
            return
        except ProcessingError as exc:  # Defensive: any other processing error.
            send_to_dlq(
                self._dlq_producer,
                self.config.dlq_topic,
                msg,
                error_type=type(exc).__name__,
                error_message=str(exc),
                attempts=1,
            )
            return

        # Success: update the real-time running average.
        self.aggregator.add(order.product, order.price)
        logger.info(
            "Processed order %s (%s @ %.2f) -> running %s",
            order.orderId,
            order.product,
            order.price,
            self.aggregator.snapshot(),
        )

    def run(self, max_messages: int | None = None) -> PriceAggregator:
        """Run the consume loop until stopped or ``max_messages`` handled."""
        self._consumer.subscribe([self.config.orders_topic])
        logger.info(
            "Consuming from %r (group=%r); DLQ=%r",
            self.config.orders_topic,
            self.config.consumer_group,
            self.config.dlq_topic,
        )
        handled = 0
        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                self._handle_message(msg)
                # Commit only after the message is fully handled (success or DLQ).
                self._consumer.commit(msg, asynchronous=False)

                handled += 1
                if max_messages is not None and handled >= max_messages:
                    logger.info("Reached max-messages=%d; stopping.", max_messages)
                    break
        finally:
            self._shutdown()
        return self.aggregator

    def _shutdown(self) -> None:
        logger.info("Flushing DLQ producer and closing consumer ...")
        self._dlq_producer.flush(timeout=10)
        self._consumer.close()
        logger.info("Final aggregate: %s", self.aggregator.snapshot())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume and process Avro order messages.")
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="stop after handling this many messages (default: run until interrupted)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = parse_args(argv)
    consumer = OrderConsumer(Config.from_env())
    # Graceful shutdown on Ctrl-C / SIGTERM. SIGTERM is not always settable on
    # Windows, so register it defensively.
    signal.signal(signal.SIGINT, consumer.stop)
    try:
        signal.signal(signal.SIGTERM, consumer.stop)
    except (ValueError, AttributeError, OSError):  # pragma: no cover - platform-specific.
        pass
    consumer.run(max_messages=args.max_messages)


if __name__ == "__main__":
    main()

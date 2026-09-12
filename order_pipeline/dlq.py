"""Dead Letter Queue publishing.

When a message can never be processed successfully - either because it is
permanently invalid, or because transient retries were exhausted - it is
forwarded to the DLQ topic. The *original* Avro-encoded value and key bytes are
preserved verbatim, and rich diagnostic context is attached as Kafka headers so
the failure can be investigated (and, if appropriate, replayed) later.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from confluent_kafka import Message, Producer

logger = logging.getLogger(__name__)


def _header(value: str) -> bytes:
    return value.encode("utf-8")


def send_to_dlq(
    producer: Producer,
    dlq_topic: str,
    original: Message,
    *,
    error_type: str,
    error_message: str,
    attempts: int,
) -> None:
    """Forward a failed message to the DLQ with diagnostic headers.

    :param producer: a plain (byte) producer; the original value is not re-encoded.
    :param original: the message that failed processing.
    :param error_type: the exception class name (e.g. ``"PermanentError"``).
    :param error_message: the human-readable failure reason.
    :param attempts: how many processing attempts were made.
    """
    headers = [
        ("x-error-type", _header(error_type)),
        ("x-error-message", _header(error_message)),
        ("x-original-topic", _header(original.topic())),
        ("x-original-partition", _header(str(original.partition()))),
        ("x-original-offset", _header(str(original.offset()))),
        ("x-attempts", _header(str(attempts))),
        ("x-failed-at", _header(datetime.now(timezone.utc).isoformat())),
    ]

    producer.produce(
        topic=dlq_topic,
        key=original.key(),
        value=original.value(),  # Preserve the exact original payload bytes.
        headers=headers,
    )
    producer.poll(0)  # Serve delivery callbacks without blocking.

    logger.error(
        "Routed message (offset=%s) to DLQ %r after %d attempt(s): %s: %s",
        original.offset(),
        dlq_topic,
        attempts,
        error_type,
        error_message,
    )

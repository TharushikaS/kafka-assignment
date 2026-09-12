"""Kafka + Avro order-processing pipeline.

A small, self-contained system that produces and consumes ``Order`` messages
over Apache Kafka using Avro serialization, and demonstrates three production
patterns required by the assignment:

* real-time aggregation (a running average of prices),
* retry with exponential backoff for transient processing failures, and
* a Dead Letter Queue (DLQ) for permanently failed messages.
"""

__version__ = "1.0.0"

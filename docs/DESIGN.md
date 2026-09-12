# Design Report — Kafka-Based Order Processing System

**Author:** Tharushika Surasinghe
**Module:** Assignment — Chapter 3
**Repository:** https://github.com/TharushikaS/kafka-assignment

---

## 1. Introduction

This report documents the design and implementation of a Kafka-based system that
produces and consumes order messages using Avro serialization. Beyond basic
messaging, the system implements three patterns commonly required of
production-grade event pipelines:

1. **Real-time aggregation** — a continuously updated running average of order
   prices;
2. **Retry logic** — automatic re-attempts of processing when *transient*
   failures occur; and
3. **A Dead Letter Queue (DLQ)** — a quarantine topic for messages that can
   never be processed successfully.

The producer and consumer are written in **Python** using the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)
client, which wraps the battle-tested `librdkafka` C library and provides
first-class Schema Registry / Avro support. The supporting infrastructure
(Kafka broker and Schema Registry) is provisioned with Docker Compose so the
whole system is reproducible with a single command.

## 2. Requirements mapping

| Requirement (brief) | Where it is implemented |
|---|---|
| Avro serialization | `schemas/order.avsc`, `AvroSerializer`/`AvroDeserializer` in `producer.py` / `consumer.py` |
| Producer of order messages | `order_pipeline/producer.py` |
| Consumer of order messages | `order_pipeline/consumer.py` |
| Real-time aggregation (running average) | `order_pipeline/aggregator.py` |
| Retry logic for temporary failures | `order_pipeline/retry.py`, `order_pipeline/processing.py` |
| Dead Letter Queue | `order_pipeline/dlq.py`, `orders.DLQ` topic |
| Git repository + live demo | this repository + `scripts/demo.*` |

## 3. Message schema

The order schema follows the brief exactly (`schemas/order.avsc`):

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.kaizens.orders",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price",   "type": "float"}
  ]
}
```

**Why Avro + Schema Registry.** Avro gives a compact binary encoding and an
explicit, versioned contract. Rather than embedding the full schema in every
message, the Confluent wire format prefixes each payload with a 5-byte header
(a magic byte + a 4-byte schema id); the reader fetches the schema from the
Registry by id and caches it. This keeps messages small, prevents producer and
consumer from silently drifting apart, and provides a controlled path for schema
evolution (e.g. adding an optional field with a default).

## 4. System architecture

See [`architecture.md`](architecture.md) for the full diagrams. In summary:

```
producer ──Avro──▶ orders topic ──▶ consumer ──┬─▶ running average (success)
                                                └─▶ orders.DLQ (permanent / exhausted)
```

* **Broker:** a single Kafka broker in **KRaft mode** (no ZooKeeper).
* **Partitioning:** the `orders` topic has 3 partitions; messages are keyed by
  `orderId`, so all events for a given order are ordered on the same partition
  while different orders spread across partitions for parallelism.
* **Consumer group:** a single logical group (`order-processors`) that can be
  scaled to up to 3 concurrent instances (one per partition).

## 5. Real-time aggregation

The consumer maintains a running average incrementally, without storing the
price history, using **Welford's online update**:

$$\bar{x}_n = \bar{x}_{n-1} + \frac{x_n - \bar{x}_{n-1}}{n}$$

This is O(1) in time and memory per message and is numerically more stable than
naively accumulating a large sum. Both a **global** average and a **per-product**
average are maintained (`PriceAggregator`), and the current snapshot is logged
after every successfully processed message. Only successfully processed, valid
orders contribute to the average — invalid orders diverted to the DLQ are
excluded, which keeps the statistic meaningful.

## 6. Failure handling: retry and DLQ

### 6.1 Classifying failures

The single most important design decision is distinguishing failures that are
worth retrying from those that are not (`order_pipeline/errors.py`):

* **`TransientError`** — a temporary condition expected to clear on its own
  (network blip, downstream timeout, transient contention). *Retry it.*
* **`PermanentError`** — a deterministic, unrecoverable condition (schema/
  validation violation, malformed business value). *Do not retry it; DLQ it.*

Retrying a permanent failure only wastes resources and delays the inevitable, so
the two are handled on separate paths.

### 6.2 Retry strategy

Transient failures are retried with **exponential backoff and jitter**
(`order_pipeline/retry.py`):

```
delay(attempt) = min(base · factor^(attempt-1), max_delay) + random_jitter
```

* **Exponential backoff** gives a struggling downstream time to recover instead
  of hammering it.
* **A cap (`max_delay`)** bounds the worst-case latency.
* **Jitter** de-synchronises many consumers so they do not retry in lockstep
  (the "thundering herd" problem).

With the defaults (`base=0.5s`, `factor=2`, `max_retries=3`) a message is tried
up to 4 times over roughly 0.5s + 1s + 2s before being declared permanently
failed.

### 6.3 Dead Letter Queue

A message is routed to the `orders.DLQ` topic when it:

1. cannot be deserialized (corrupt payload),
2. raises a `PermanentError` (failed validation), or
3. exhausts its retry budget (`RetriesExhausted`).

The DLQ record **preserves the original key and value bytes verbatim** and
attaches diagnostic **Kafka headers** so each failure is fully explainable and
potentially replayable:

| Header | Meaning |
|---|---|
| `x-error-type` | exception class (`PermanentError`, `RetriesExhausted`, …) |
| `x-error-message` | human-readable reason |
| `x-original-topic` / `-partition` / `-offset` | provenance of the message |
| `x-attempts` | number of processing attempts made |
| `x-failed-at` | UTC timestamp of the failure |

`order_pipeline/dlq_inspector.py` drains and pretty-prints the DLQ for the demo.

## 7. Delivery semantics

The consumer disables auto-commit and **commits offsets manually, only after a
message has been fully handled** (either processed or safely written to the
DLQ). This gives **at-least-once** delivery: if the consumer crashes mid-way, the
uncommitted message is simply redelivered. Because the DLQ write happens before
the commit, no failed message is ever silently dropped. (Exactly-once would
require Kafka transactions spanning the DLQ write and the offset commit; it was
considered out of scope but is noted below as future work.)

## 8. Testing

Deterministic unit tests (`tests/`, run with `pytest`) cover the pure logic in
isolation from Kafka, using injected clocks and seeded RNGs:

* `test_aggregator.py` — the running average is correct after every update, both
  globally and per product.
* `test_retry.py` — backoff is exponential and capped; permanent errors are not
  retried; transient-then-success recovers; exhaustion raises `RetriesExhausted`.
* `test_processing.py` — validation produces `PermanentError`; the transient
  failure injector fires as configured.
* `test_models.py` — the model round-trips through its Avro dict form and the
  schema matches the model fields.

The full round trip through a real broker is exercised by the demo script and
was verified manually during development.

## 9. Limitations and future work

* **Exactly-once semantics.** Wrap the DLQ produce and the offset commit in a
  Kafka transaction to eliminate the (currently harmless) possibility of a
  duplicate on redelivery.
* **DLQ replay tooling.** Add a command to re-inject repaired DLQ messages back
  into the `orders` topic.
* **Persistent / windowed aggregation.** The running average is in-memory and
  resets when the consumer restarts; a durable store (or Kafka Streams with a
  state store) would survive restarts and support time-windowed statistics.
* **Observability.** Export metrics (throughput, retry counts, DLQ rate) to
  Prometheus/Grafana.

## 10. References

* Apache Kafka documentation — https://kafka.apache.org/documentation/
* Confluent Schema Registry & Avro — https://docs.confluent.io/platform/current/schema-registry/
* `confluent-kafka-python` — https://docs.confluent.io/platform/current/clients/confluent-kafka-python/
* B. P. Welford (1962), "Note on a method for calculating corrected sums of
  squares and products", *Technometrics* 4(3).
* AWS Architecture Blog — exponential backoff and jitter.

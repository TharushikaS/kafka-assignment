# OrderStream — Kafka-Based Order Processing System

**OrderStream** is a Kafka pipeline that **produces** and **consumes** order messages with **Avro**
serialization, and demonstrates three production patterns:

- 📊 **Real-time aggregation** — a running average of order prices (global + per product)
- 🔁 **Retry logic** — exponential backoff with jitter for *transient* failures
- ☠️ **Dead Letter Queue (DLQ)** — quarantine for *permanently* failed messages

Built in **Python** with `confluent-kafka`; Kafka + Schema Registry run locally
via **Docker Compose**.

> 📄 Full design rationale is in [`docs/DESIGN.md`](docs/DESIGN.md); diagrams are
> in [`docs/architecture.md`](docs/architecture.md); a timed script for the
> live-demo video is in [`docs/DEMO.md`](docs/DEMO.md).

---

## Architecture at a glance

```
                 ┌──────────────┐   registers/looks up schema   ┌───────────────────┐
                 │   Producer   │◀─────────────────────────────▶│  Schema Registry  │
                 └──────┬───────┘                                └───────────────────┘
             Avro-encoded, keyed by orderId                               ▲
                        ▼                                                  │
                 ┌──────────────┐        ┌───────────────┐                │
                 │ topic:orders │───────▶│   Consumer    │────────────────┘
                 │ (3 partitions)│       │  deserialize  │
                 └──────────────┘        │  + process    │
                                         └──┬────────┬───┘
                       success ────────────┘        └──────── permanent / retries exhausted
                          ▼                                        ▼
                 ┌──────────────────┐                     ┌────────────────┐
                 │ running average   │                     │ topic:orders.DLQ│
                 │ (global + product)│                     │  + error headers│
                 └──────────────────┘                     └────────────────┘
```

## Project layout

```
kafka-assignment/
├── docker-compose.yml         # Kafka (KRaft) + Schema Registry + Kafka UI
├── requirements.txt
├── .env.example               # all tunables, with defaults
├── schemas/
│   └── order.avsc             # the Avro schema from the brief
├── order_pipeline/
│   ├── config.py              # env-driven configuration
│   ├── models.py              # Order model + Avro dict hooks
│   ├── schemas.py             # schema loading
│   ├── errors.py              # Transient / Permanent / RetriesExhausted
│   ├── aggregator.py          # running average (Welford)
│   ├── retry.py               # exponential backoff + jitter
│   ├── processing.py          # business logic + failure injection
│   ├── dlq.py                 # DLQ publishing with diagnostic headers
│   ├── admin.py               # topic provisioning
│   ├── producer.py            # produces Avro order messages
│   ├── consumer.py            # consumes, retries, aggregates, DLQs
│   └── dlq_inspector.py       # reads & prints the DLQ
├── tests/                     # pytest unit tests (no broker needed)
├── scripts/                   # demo.ps1 / demo.sh
├── docs/                      # DESIGN.md + architecture.md
└── Makefile
```

## Prerequisites

- **Docker Desktop** (for Kafka + Schema Registry)
- **Python 3.10+**

## Quick start

### 1. Start the infrastructure

```bash
docker compose up -d
```

This starts a Kafka broker (`localhost:9092`), the Schema Registry
(`localhost:8081`), and a web UI at **http://localhost:8080**. Wait ~20s for the
containers to report healthy (`docker compose ps`).

### 2. Install Python dependencies

```bash
python -m venv .venv
# Windows:  .venv\Scripts\activate
# Linux/macOS:  source .venv/bin/activate
pip install -r requirements.txt
```

Optionally, copy the environment defaults: `cp .env.example .env`.

### 3. Create the topics

```bash
python -m order_pipeline.admin
```

### 4. Run it

In one terminal, start the consumer:

```bash
python -m order_pipeline.consumer
```

In another, produce some orders (5% of them deliberately invalid, to exercise
the DLQ):

```bash
python -m order_pipeline.producer --count 200 --invalid-rate 0.05 --rate 50 --seed 42
```

Watch the consumer log the **running average** update after each message, retry
**transient** failures, and route **permanent** failures to the DLQ.

### 5. Inspect the Dead Letter Queue

```bash
python -m order_pipeline.dlq_inspector
```

### One-shot demo

Everything above, end to end (the stack must already be up):

```bash
# Windows
./scripts/demo.ps1
# Linux/macOS/Git Bash
./scripts/demo.sh
```

## Command reference

| Command | Purpose |
|---|---|
| `python -m order_pipeline.admin` | create the `orders` and `orders.DLQ` topics |
| `python -m order_pipeline.producer [--count N] [--invalid-rate F] [--rate R] [--seed S]` | produce random orders |
| `python -m order_pipeline.consumer [--max-messages N]` | consume, process, aggregate, DLQ |
| `python -m order_pipeline.dlq_inspector [--timeout S]` | print DLQ contents + error headers |

All behaviour is configurable via environment variables — see
[`.env.example`](.env.example) for the full list (broker address, topics, retry
policy, failure-injection rates, price validation bound).

## How the required features work

### Avro serialization
Messages are serialized with `AvroSerializer` against the Schema Registry using
`schemas/order.avsc`. Each payload carries only a schema id, not the full schema.
The consumer deserializes with the matching `AvroDeserializer`.

### Real-time aggregation
The consumer keeps an incremental (Welford) running average of prices — globally
and per product — updated in O(1) per message and logged after each success. See
`order_pipeline/aggregator.py`.

### Retry logic
Processing failures are classified as **transient** (retryable) or **permanent**
(not). Transient failures are retried with exponential backoff + jitter
(`order_pipeline/retry.py`); the rate is configurable via `TRANSIENT_FAILURE_RATE`
for demonstration.

### Dead Letter Queue
Undecodable payloads, permanent validation failures, and retry-exhausted messages
are forwarded to `orders.DLQ` with their original bytes plus diagnostic headers
(`x-error-type`, `x-error-message`, `x-attempts`, provenance, timestamp). Offsets
are committed only *after* a message is handled, so nothing is lost.

## Testing

The pure logic (aggregation, retry, validation, model round-trip) is covered by
deterministic unit tests that need **no running broker**:

```bash
pip install -r requirements.txt
pytest
```

## Tear down

```bash
docker compose down        # stop containers
docker compose down -v     # also delete Kafka data volumes
```

## Tech stack & references

- Apache Kafka (KRaft mode), Confluent Schema Registry
- `confluent-kafka-python`, `fastavro`
- See [`docs/DESIGN.md`](docs/DESIGN.md) §10 for full references.

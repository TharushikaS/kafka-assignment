# Architecture

## Component / data-flow diagram

```mermaid
flowchart LR
    subgraph Producer["Producer (order_pipeline.producer)"]
        GEN["Generate random Order\n(orderId, product, price)"]
        ASER["Avro serialize\n(value)"]
        GEN --> ASER
    end

    SR[("Schema Registry\norder.avsc")]

    subgraph Broker["Kafka (KRaft)"]
        OT[["topic: orders\n(3 partitions)"]]
        DLQ[["topic: orders.DLQ"]]
    end

    subgraph Consumer["Consumer (order_pipeline.consumer)"]
        ADES["Avro deserialize"]
        PROC{"process_order\nvalidate + I/O"}
        RETRY["retry w/ backoff\n(transient only)"]
        AGG[["Running average\n(global + per product)"]]
        COMMIT["Manual offset commit"]
        ADES --> PROC
        PROC -->|transient| RETRY
        RETRY -->|recovered| AGG
        PROC -->|success| AGG
        AGG --> COMMIT
    end

    ASER -->|register / lookup schema| SR
    ASER -->|produce keyed by orderId| OT
    OT --> ADES
    ADES -->|schema lookup| SR

    PROC -->|permanent error| DLQ
    RETRY -->|retries exhausted| DLQ
    ADES -->|undecodable payload| DLQ
    DLQ -.->|committed too| COMMIT

    INSPECT["dlq_inspector"] -.->|drains & prints| DLQ
```

## Message-handling decision flow

```mermaid
flowchart TD
    A["Poll message"] --> B{"Deserialize OK?"}
    B -- no --> DLQ["Send to DLQ\n(DeserializationError)"]
    B -- yes --> C["process_order"]
    C --> D{"Error type?"}
    D -- "none (success)" --> E["Update running average"]
    D -- "PermanentError" --> DLQ2["Send to DLQ\n(PermanentError)"]
    D -- "TransientError" --> F{"attempts < max_retries?"}
    F -- yes --> G["Backoff + retry"]
    G --> C
    F -- no --> DLQ3["Send to DLQ\n(RetriesExhausted)"]
    E --> H["Commit offset"]
    DLQ --> H
    DLQ2 --> H
    DLQ3 --> H
    H --> A
```

## Why these choices

* **KRaft mode (no ZooKeeper):** the current, simplest single-broker setup for
  modern Kafka (7.x); fewer moving parts for a local demo.
* **Schema Registry + Avro:** the schema is registered once and referenced by id
  in each message, so payloads stay small and producers/consumers cannot drift
  out of sync. It also enables controlled schema evolution.
* **Key = `orderId`:** guarantees per-order ordering by pinning all events for an
  order to one partition, while still allowing horizontal scale across orders.
* **Manual offset commit after handling:** yields at-least-once semantics with no
  message loss — an offset is only advanced once the message has been processed
  or safely parked in the DLQ.
* **Separate DLQ topic with header metadata:** keeps the main stream clean while
  preserving the exact failed payload plus the context needed to diagnose or
  replay it.

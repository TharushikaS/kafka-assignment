# Live Demonstration Runbook (≤ 5 minutes)

A tight, rehearsed script for the submission video. It shows **every** required
feature: Avro serialization, real-time aggregation, retry logic, and the DLQ.

> **Tip:** Do all "Before recording" steps off-camera so the recording opens on
> a clean, ready system and every on-camera second shows something interesting.

---

## Before recording (off-camera, ~1 min)

1. **Reset to a clean slate** (fresh broker, empty topics, empty DLQ):

   ```bash
   # Windows
   ./scripts/reset.ps1
   # Linux/macOS/Git Bash
   ./scripts/reset.sh
   ```

   This tears the stack down (with volumes), starts it again, waits for the
   broker + Schema Registry to be healthy, and creates the topics.

2. **Activate your Python environment** and confirm dependencies are installed:

   ```bash
   # Windows:  .venv\Scripts\activate
   # Linux/macOS:  source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Open two terminals** side by side (one for the *consumer*, one for the
   *producer*) and a browser tab at **http://localhost:8080** (Kafka UI). The
   cluster appears there as **OrderStream**.

   > **Light theme:** Kafka UI follows your operating system's light/dark
   > setting and cannot be forced from Docker. For a light UI on camera, either
   > set your OS to light mode, or click the **sun/moon toggle** in the top-right
   > of the Kafka UI header (the choice is remembered in that browser).

4. **Set a higher failure rate** so retries are clearly visible on camera. In the
   **consumer** terminal only:

   ```bash
   # Windows PowerShell
   $env:TRANSIENT_FAILURE_RATE = "0.3"
   # Linux/macOS
   export TRANSIENT_FAILURE_RATE=0.3
   ```

---

## On camera (~4.5 min)

### 0:00 – 0:40 · Introduction
- One sentence on the goal: *"A Kafka pipeline that produces and consumes order
  messages with Avro, and adds a running-average aggregation, retry logic, and a
  Dead Letter Queue."*
- Show the **README** and the **architecture diagram** (`docs/architecture.md`).

### 0:40 – 1:10 · Infrastructure & schema
- Show `docker compose ps` — Kafka, Schema Registry, and Kafka UI all **up**.
- Show `schemas/order.avsc` — the Avro record (`orderId`, `product`, `price`).
- Mention topics were created by `python -m order_pipeline.admin`.

### 1:10 – 1:40 · Start the consumer
In the **consumer** terminal:

```bash
python -m order_pipeline.consumer
```

Point out it is now waiting for messages, and that it commits offsets manually
(at-least-once, no message loss).

### 1:40 – 3:10 · Produce orders → live aggregation + retries
In the **producer** terminal:

```bash
python -m order_pipeline.producer --count 60 --invalid-rate 0.1 --rate 12 --seed 7
```

Narrate what the consumer window shows in real time:
- **Avro** messages being deserialized and processed.
- The **running average** (`avg=...`) updating after every message, plus the
  **per-product** breakdown.
- **Retry** lines: `Transient failure ... retry 1/3 in 0.55s` followed by the
  message succeeding — this is the retry logic working.

### 3:10 – 4:10 · Dead Letter Queue
- Point out the **red ERROR** lines: `Routed message ... to DLQ 'orders.DLQ' ...
  PermanentError: invalid price ...` — invalid orders (negative price) go
  straight to the DLQ without retries.
- Stop the consumer with **Ctrl-C** (note the graceful shutdown + final
  aggregate line).
- Show the DLQ contents with the inspector:

  ```bash
  python -m order_pipeline.dlq_inspector
  ```

  Highlight, for one record: the preserved order, the `reason`
  (`PermanentError`), and the diagnostic headers (`origin`, `attempts`,
  `failed_at`).
- *(Optional)* In **Kafka UI** (http://localhost:8080), open **Topics → orders**
  and **orders.DLQ** to show the messages and the registered Avro schema.

### 4:10 – 4:40 · Tests
```bash
pytest -q
```
Show the suite passing (aggregation, retry classification, validation, model
round-trip) — proof the logic is correct independently of the broker.

### 4:40 – 5:00 · Wrap up
Recap the four requirements met — **Avro, running average, retry, DLQ** — and
point to the GitHub repository:
`https://github.com/TharushikaS/kafka-assignment`.

---

## Fallback (single-terminal / very tight on time)

If you prefer one terminal and a bounded run, produce first, then consume a fixed
number of messages so the run ends on its own:

```bash
python -m order_pipeline.producer --count 60 --invalid-rate 0.1 --rate 0 --seed 7
python -m order_pipeline.consumer --max-messages 60
python -m order_pipeline.dlq_inspector
```

## Quick reference — what each feature looks like on screen

| Feature | What to point at |
|---|---|
| Avro serialization | `orders-value` schema in Kafka UI / Schema Registry; messages decode cleanly |
| Running average | `avg=...` and per-product `ItemN:avg=...` updating each message |
| Retry logic | `WARNING ... Transient failure ... retry 1/3` then the order is processed |
| Dead Letter Queue | `ERROR ... Routed message ... to DLQ` + `dlq_inspector` output with headers |

"""Helpers for locating and loading the Avro schema."""

from __future__ import annotations

from pathlib import Path

# schemas/order.avsc lives next to the package, at the project root.
SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"
ORDER_SCHEMA_PATH = SCHEMA_DIR / "order.avsc"


def load_order_schema() -> str:
    """Return the ``order.avsc`` schema as a JSON string.

    The confluent-kafka Avro serdes take the schema as a string, which they
    register with (or look up in) the Schema Registry.
    """
    return ORDER_SCHEMA_PATH.read_text(encoding="utf-8")

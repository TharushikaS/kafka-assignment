"""The :class:`Order` domain model and its Avro (de)serialization hooks.

The ``to_dict`` / ``from_dict`` functions are the bridge between the Python
object and the plain ``dict`` shape that the Avro serializer expects. They match
the ``schemas/order.avsc`` record exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Order:
    """A purchase transaction, mirroring ``order.avsc``."""

    orderId: str
    product: str
    price: float


def order_to_dict(order: Order, ctx: Any = None) -> dict[str, Any]:
    """Convert an :class:`Order` to the dict shape Avro expects.

    The ``ctx`` argument (a ``SerializationContext``) is required by the
    confluent-kafka ``AvroSerializer`` callback signature but is unused here.
    """
    return {"orderId": order.orderId, "product": order.product, "price": order.price}


def dict_to_order(obj: dict[str, Any] | None, ctx: Any = None) -> Order | None:
    """Rebuild an :class:`Order` from a decoded Avro dict."""
    if obj is None:
        return None
    return Order(orderId=obj["orderId"], product=obj["product"], price=float(obj["price"]))

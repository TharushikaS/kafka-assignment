"""Unit tests for the Order model and its Avro (de)serialization hooks."""

from __future__ import annotations

import json

import pytest

from order_pipeline.models import Order, dict_to_order, order_to_dict
from order_pipeline.schemas import load_order_schema


def test_order_round_trips_through_dict():
    order = Order(orderId="1001", product="Item1", price=12.5)
    as_dict = order_to_dict(order)
    assert as_dict == {"orderId": "1001", "product": "Item1", "price": 12.5}
    assert dict_to_order(as_dict) == order


def test_dict_to_order_handles_none():
    assert dict_to_order(None) is None


def test_dict_to_order_coerces_price_to_float():
    order = dict_to_order({"orderId": "1", "product": "Item1", "price": 10})
    assert isinstance(order.price, float)
    assert order.price == 10.0


def test_avro_schema_matches_model_fields():
    schema = json.loads(load_order_schema())
    assert schema["name"] == "Order"
    field_names = {f["name"] for f in schema["fields"]}
    assert field_names == {"orderId", "product", "price"}
    types = {f["name"]: f["type"] for f in schema["fields"]}
    assert types == {"orderId": "string", "product": "string", "price": "float"}

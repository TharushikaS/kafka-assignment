"""Unit tests for the order-processing business logic."""

from __future__ import annotations

import random

import pytest

from order_pipeline.config import Config
from order_pipeline.errors import PermanentError, TransientError
from order_pipeline.models import Order
from order_pipeline.processing import process_order


@pytest.fixture
def config() -> Config:
    return Config(transient_failure_rate=0.0, max_price=1000.0)


def test_valid_order_succeeds(config: Config):
    order = Order(orderId="1001", product="Item1", price=99.0)
    # transient_failure_rate=0 -> never raises; should simply return.
    assert process_order(order, config, rng=random.Random(0)) is None


def test_negative_price_is_permanent(config: Config):
    order = Order(orderId="1002", product="Item1", price=-5.0)
    with pytest.raises(PermanentError):
        process_order(order, config)


def test_zero_price_is_permanent(config: Config):
    order = Order(orderId="1003", product="Item1", price=0.0)
    with pytest.raises(PermanentError):
        process_order(order, config)


def test_price_above_max_is_permanent(config: Config):
    order = Order(orderId="1004", product="Item1", price=5000.0)
    with pytest.raises(PermanentError):
        process_order(order, config)


def test_transient_failure_is_injected():
    # With rate 1.0 a valid order always raises a transient error.
    config = Config(transient_failure_rate=1.0, max_price=1000.0)
    order = Order(orderId="1005", product="Item1", price=50.0)
    with pytest.raises(TransientError):
        process_order(order, config, rng=random.Random(0))

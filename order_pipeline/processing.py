"""Business logic for processing a single order.

This is where a real system would enrich, validate and persist the order. Here
it does two things that let us demonstrate the required failure-handling:

1. **Validation** -> a :class:`PermanentError` for orders that violate the
   business rules (non-positive price, or a price above ``max_price``). These
   are genuinely unrecoverable and must not be retried.
2. **Simulated I/O** -> a :class:`TransientError` raised at random with
   probability ``transient_failure_rate``, imitating a flaky downstream
   dependency so retries (and, when unlucky, DLQ-after-exhaustion) can be seen.
"""

from __future__ import annotations

import random

from .config import Config
from .errors import PermanentError, TransientError
from .models import Order


def process_order(
    order: Order,
    config: Config,
    rng: random.Random | None = None,
) -> None:
    """Process one order, or raise a Transient/Permanent error.

    :param rng: injectable random source so tests are deterministic.
    """
    rng = rng or random

    # --- 1. Validation: permanent, non-retryable failures. ---
    if order.price <= 0:
        raise PermanentError(
            f"invalid price {order.price!r} for order {order.orderId!r}: must be > 0"
        )
    if order.price > config.max_price:
        raise PermanentError(
            f"price {order.price!r} for order {order.orderId!r} exceeds "
            f"max_price {config.max_price!r}"
        )

    # --- 2. Simulated downstream I/O: transient, retryable failure. ---
    if rng.random() < config.transient_failure_rate:
        raise TransientError(
            f"downstream service unavailable while processing order {order.orderId!r}"
        )

    # --- Success: a real system would persist / forward the order here. ---

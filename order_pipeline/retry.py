"""Retry with exponential backoff and jitter.

Only :class:`~order_pipeline.errors.TransientError` is retried. A
:class:`~order_pipeline.errors.PermanentError` propagates immediately (no point
retrying something that can never succeed). When the transient error persists
past ``max_retries``, a :class:`~order_pipeline.errors.RetriesExhausted` is
raised so the caller can route the message to the DLQ.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, TypeVar

from .config import Config
from .errors import PermanentError, RetriesExhausted, TransientError

logger = logging.getLogger(__name__)

T = TypeVar("T")


def compute_backoff(attempt: int, config: Config, rng: random.Random | None = None) -> float:
    """Return the delay (seconds) before the given retry ``attempt`` (1-based).

    Exponential backoff, capped at ``retry_max_delay``, with *full* jitter of up
    to ``retry_jitter`` of the base delay added to avoid thundering-herd retries.
    """
    rng = rng or random
    exponential = config.retry_base_delay * (config.retry_backoff_factor ** (attempt - 1))
    capped = min(exponential, config.retry_max_delay)
    jitter = rng.uniform(0, config.retry_jitter * config.retry_base_delay)
    return capped + jitter


def run_with_retry(
    func: Callable[[], T],
    config: Config,
    *,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> T:
    """Call ``func`` with retries on :class:`TransientError`.

    :param func: a zero-argument callable performing the unit of work.
    :param config: retry policy (max retries, delays, jitter).
    :param sleep: injectable sleep function (tests pass a no-op).
    :param rng: injectable random source (tests pass a seeded one).
    :raises PermanentError: immediately, if ``func`` raises one.
    :raises RetriesExhausted: if transient failures outlast ``max_retries``.
    """
    attempt = 0
    while True:
        try:
            return func()
        except PermanentError:
            # Never retry a permanent failure.
            raise
        except TransientError as exc:
            if attempt >= config.max_retries:
                raise RetriesExhausted(attempts=attempt + 1, last_error=exc) from exc
            attempt += 1
            delay = compute_backoff(attempt, config, rng=rng)
            logger.warning(
                "Transient failure (%s); retry %d/%d in %.2fs: %s",
                type(exc).__name__,
                attempt,
                config.max_retries,
                delay,
                exc,
            )
            sleep(delay)

"""Unit tests for the retry/backoff policy."""

from __future__ import annotations

import random

import pytest

from order_pipeline.config import Config
from order_pipeline.errors import PermanentError, RetriesExhausted, TransientError
from order_pipeline.retry import compute_backoff, run_with_retry


@pytest.fixture
def config() -> Config:
    # No jitter -> deterministic backoff values; small retry budget.
    return Config(max_retries=3, retry_base_delay=1.0, retry_backoff_factor=2.0,
                  retry_max_delay=8.0, retry_jitter=0.0)


def test_backoff_is_exponential_and_capped(config: Config):
    rng = random.Random(0)
    assert compute_backoff(1, config, rng=rng) == pytest.approx(1.0)
    assert compute_backoff(2, config, rng=rng) == pytest.approx(2.0)
    assert compute_backoff(3, config, rng=rng) == pytest.approx(4.0)
    # 1.0 * 2^4 = 16 -> capped at retry_max_delay (8.0).
    assert compute_backoff(5, config, rng=rng) == pytest.approx(8.0)


def test_success_on_first_attempt_does_not_sleep(config: Config):
    sleeps: list[float] = []
    result = run_with_retry(lambda: "ok", config, sleep=sleeps.append)
    assert result == "ok"
    assert sleeps == []


def test_transient_then_success(config: Config):
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("temporary")
        return "recovered"

    sleeps: list[float] = []
    result = run_with_retry(flaky, config, sleep=sleeps.append, rng=random.Random(0))
    assert result == "recovered"
    assert calls["n"] == 3
    assert len(sleeps) == 2  # Two retries before the third, successful attempt.


def test_permanent_error_is_not_retried(config: Config):
    calls = {"n": 0}

    def always_permanent() -> None:
        calls["n"] += 1
        raise PermanentError("invalid")

    sleeps: list[float] = []
    with pytest.raises(PermanentError):
        run_with_retry(always_permanent, config, sleep=sleeps.append)
    assert calls["n"] == 1  # Called exactly once, no retries.
    assert sleeps == []


def test_retries_exhausted(config: Config):
    calls = {"n": 0}

    def always_transient() -> None:
        calls["n"] += 1
        raise TransientError("still down")

    sleeps: list[float] = []
    with pytest.raises(RetriesExhausted) as exc_info:
        run_with_retry(always_transient, config, sleep=sleeps.append, rng=random.Random(0))

    # 1 initial attempt + max_retries retries.
    assert calls["n"] == config.max_retries + 1
    assert exc_info.value.attempts == config.max_retries + 1
    assert isinstance(exc_info.value.last_error, TransientError)

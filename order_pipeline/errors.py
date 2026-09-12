"""Exception hierarchy that drives the retry / DLQ decision.

The distinction between *transient* and *permanent* failures is the single most
important design decision in the consumer: it decides whether a message is
retried or sent straight to the Dead Letter Queue.
"""

from __future__ import annotations


class ProcessingError(Exception):
    """Base class for all message-processing failures."""


class TransientError(ProcessingError):
    """A temporary failure that is expected to succeed if retried.

    Examples: a downstream service timing out, a momentary network blip, or a
    database deadlock. These are retried with exponential backoff.
    """


class PermanentError(ProcessingError):
    """A failure that will never succeed no matter how many times it is retried.

    Examples: a schema/validation violation or a malformed business value.
    These bypass retries and go straight to the DLQ.
    """


class RetriesExhausted(ProcessingError):
    """Raised when a :class:`TransientError` keeps failing past ``max_retries``.

    Carries the number of attempts made and the last underlying error so the
    DLQ record can explain exactly what happened.
    """

    def __init__(self, attempts: int, last_error: Exception) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Retries exhausted after {attempts} attempt(s); "
            f"last error: {type(last_error).__name__}: {last_error}"
        )

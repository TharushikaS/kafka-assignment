"""Shared logging setup so every entry point logs consistently."""

from __future__ import annotations

import logging
import os


def configure_logging() -> None:
    """Configure root logging once, honouring the ``LOG_LEVEL`` env var."""
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

"""Real-time aggregation of order prices.

Maintains a *running* average that is updated incrementally as each message is
consumed, without storing the full history of prices. A global average is kept
alongside a per-product breakdown.

The incremental mean uses Welford's update rule::

    mean_n = mean_{n-1} + (x_n - mean_{n-1}) / n

which is numerically stable and O(1) in both time and memory per message.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunningStat:
    """A single incremental mean, with count and sum tracked alongside."""

    count: int = 0
    total: float = 0.0
    mean: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        # Welford's incremental mean update.
        self.mean += (value - self.mean) / self.count


@dataclass
class PriceAggregator:
    """Tracks the running average price globally and per product."""

    overall: RunningStat = field(default_factory=RunningStat)
    per_product: dict[str, RunningStat] = field(default_factory=dict)

    def add(self, product: str, price: float) -> None:
        """Fold one successfully processed order into the aggregates."""
        self.overall.add(price)
        self.per_product.setdefault(product, RunningStat()).add(price)

    @property
    def count(self) -> int:
        return self.overall.count

    @property
    def average(self) -> float:
        return self.overall.mean

    def snapshot(self) -> str:
        """A compact, human-readable one-line summary for logging."""
        parts = [
            f"count={self.overall.count}",
            f"avg={self.overall.mean:.2f}",
        ]
        for product in sorted(self.per_product):
            stat = self.per_product[product]
            parts.append(f"{product}:avg={stat.mean:.2f}(n={stat.count})")
        return " | ".join(parts)

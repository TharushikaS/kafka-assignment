"""Unit tests for the real-time running-average aggregator."""

from __future__ import annotations

import pytest

from order_pipeline.aggregator import PriceAggregator, RunningStat


def test_running_stat_matches_plain_mean():
    stat = RunningStat()
    values = [10.0, 20.0, 30.0, 40.0]
    for v in values:
        stat.add(v)
    assert stat.count == 4
    assert stat.total == pytest.approx(100.0)
    assert stat.mean == pytest.approx(25.0)


def test_running_stat_is_incremental():
    # The mean must be correct after *every* update, not just at the end.
    stat = RunningStat()
    stat.add(4.0)
    assert stat.mean == pytest.approx(4.0)
    stat.add(6.0)
    assert stat.mean == pytest.approx(5.0)
    stat.add(20.0)
    assert stat.mean == pytest.approx(10.0)


def test_aggregator_tracks_global_and_per_product():
    agg = PriceAggregator()
    agg.add("Item1", 100.0)
    agg.add("Item2", 200.0)
    agg.add("Item1", 300.0)

    assert agg.count == 3
    assert agg.average == pytest.approx(200.0)
    assert agg.per_product["Item1"].mean == pytest.approx(200.0)
    assert agg.per_product["Item1"].count == 2
    assert agg.per_product["Item2"].mean == pytest.approx(200.0)
    assert agg.per_product["Item2"].count == 1


def test_snapshot_is_readable():
    agg = PriceAggregator()
    agg.add("Item1", 10.0)
    snapshot = agg.snapshot()
    assert "count=1" in snapshot
    assert "avg=10.00" in snapshot
    assert "Item1" in snapshot

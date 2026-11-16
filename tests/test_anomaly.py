from datetime import datetime, timedelta

import pytest

from streaming.anomaly import detect_traffic_anomalies

WINDOW_START = datetime(2026, 11, 16, 12, 0, 0)


def events(category: str, count: int, users: int):
    """`count` events spread over one minute, round-robin across `users` users."""
    return [
        (f"{category}-{i}", f"user_{i % users}", "view_offer", category, 10.0,
         WINDOW_START + timedelta(seconds=i * 59 / max(count, 1)))
        for i in range(count)
    ]


def detect(spark, rows):
    df = spark.createDataFrame(
        rows, ["event_id", "user_id", "event_type", "category", "price", "event_ts"]
    )
    # Tumbling 1-minute windows keep the expected counts easy to reason about.
    result = detect_traffic_anomalies(
        df,
        window_duration="1 minute",
        slide_duration="1 minute",
        spike_threshold=300,
        drop_threshold=20,
        bot_ratio_threshold=3.0,
    )
    return {r.category: r for r in result.collect()}


def test_traffic_classification(spark):
    rows = (
        events("fashion", count=150, users=140)       # normal
        + events("electronics", count=500, users=450)  # spike
        + events("automotive", count=5, users=5)       # drop
        + events("home_garden", count=200, users=10)   # 20 events/user -> bot
    )
    result = detect(spark, rows)

    assert result["fashion"].traffic_status == "NORMAL"
    assert result["electronics"].traffic_status == "SPIKE"
    assert result["automotive"].traffic_status == "DROP"
    assert result["home_garden"].traffic_status == "BOT_SUSPECTED"


def test_aggregates(spark):
    result = detect(spark, events("fashion", count=40, users=40))["fashion"]

    assert result.event_count == 40
    # approx_count_distinct (HyperLogLog++) has ~5% relative standard error by default
    assert result.unique_users == pytest.approx(40, rel=0.1)
    assert result.events_per_user == pytest.approx(1.0, rel=0.1)
    assert result.avg_price == 10.0
    assert result.window_start == WINDOW_START
    assert result.window_end == WINDOW_START + timedelta(minutes=1)

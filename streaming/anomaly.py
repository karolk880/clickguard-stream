"""Windowed traffic anomaly detection on validated clickstream events."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

NORMAL = "NORMAL"


def detect_traffic_anomalies(
    valid_events: DataFrame,
    window_duration: str = "1 minute",
    slide_duration: str = "30 seconds",
    spike_threshold: int = 300,
    drop_threshold: int = 20,
    bot_ratio_threshold: float = 3.0,
) -> DataFrame:
    """Aggregate events per (sliding window, category) and classify the traffic.

    For streaming input the caller must apply `withWatermark("event_ts", ...)`
    first, otherwise Spark keeps window state forever.

    Statuses (first match wins):
      * BOT_SUSPECTED - many events per distinct user (scraper / bot burst)
      * SPIKE         - total volume above `spike_threshold`
      * DROP          - total volume below `drop_threshold` (e.g. broken tracking)
      * NORMAL
    """
    return (
        valid_events.groupBy(
            F.window("event_ts", window_duration, slide_duration).alias("window"),
            F.col("category"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
        )
        .withColumn("events_per_user", F.round(F.col("event_count") / F.col("unique_users"), 2))
        .withColumn(
            "traffic_status",
            F.when(F.col("events_per_user") >= bot_ratio_threshold, F.lit("BOT_SUSPECTED"))
            .when(F.col("event_count") > spike_threshold, F.lit("SPIKE"))
            .when(F.col("event_count") < drop_threshold, F.lit("DROP"))
            .otherwise(F.lit(NORMAL)),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            "event_count",
            "unique_users",
            "events_per_user",
            "purchase_count",
            "avg_price",
            "traffic_status",
        )
    )

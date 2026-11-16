"""ClickGuard Stream - main Spark Structured Streaming job.

Run from the repository root:
    python -m streaming.stream_processor
"""

import os

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.anomaly import NORMAL, detect_traffic_anomalies
from streaming.validation import validate_events

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "clickstream-raw")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "clickstream-dlq")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "clickstream-alerts")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./output/clean_clickstream")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "./checkpoints")

ANOMALY_WATERMARK = os.getenv("ANOMALY_WATERMARK", "2 minutes")
DEDUP_WATERMARK = os.getenv("DEDUP_WATERMARK", "10 minutes")
SPIKE_THRESHOLD = int(os.getenv("SPIKE_THRESHOLD", "300"))
DROP_THRESHOLD = int(os.getenv("DROP_THRESHOLD", "20"))
BOT_RATIO_THRESHOLD = float(os.getenv("BOT_RATIO_THRESHOLD", "3.0"))


def kafka_connector_package() -> str:
    """The Kafka connector must match the installed Spark and Scala version."""
    scala = "2.13" if pyspark.__version__.startswith("4") else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ClickGuard-Stream-Validator")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", kafka_connector_package())
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_raw_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10_000)  # back-pressure
        .option("failOnDataLoss", "false")  # local dev only
        .load()
    )


def as_kafka_value(df: DataFrame) -> DataFrame:
    return df.select(F.to_json(F.struct("*")).alias("value"))


def publish_alerts(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: print non-NORMAL windows and publish them to Kafka."""
    alerts = batch_df.filter(F.col("traffic_status") != NORMAL).persist()
    try:
        if alerts.isEmpty():
            return
        print(f"--- anomaly alerts (batch {batch_id}) ---")
        alerts.orderBy("window_start", "category").show(truncate=False)
        (
            as_kafka_value(alerts)
            .write.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("topic", ALERTS_TOPIC)
            .save()
        )
    finally:
        alerts.unpersist()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    valid_events, invalid_events = validate_events(read_raw_stream(spark))

    # 1. Dead Letter Queue - rejected events with reason + Kafka coordinates
    (
        as_kafka_value(invalid_events)
        .writeStream.queryName("dlq")
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .start()
    )

    # 2. Anomaly detection. Append mode emits each window once, after the
    #    watermark closes it - no false DROP alerts from half-filled windows.
    anomalies = detect_traffic_anomalies(
        valid_events.withWatermark("event_ts", ANOMALY_WATERMARK),
        spike_threshold=SPIKE_THRESHOLD,
        drop_threshold=DROP_THRESHOLD,
        bot_ratio_threshold=BOT_RATIO_THRESHOLD,
    )
    (
        anomalies.writeStream.queryName("anomalies")
        .outputMode("append")
        .foreachBatch(publish_alerts)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomalies")
        .start()
    )

    # 3. Clean layer - deduplicated (client retries) and partitioned by date/type
    (
        valid_events.withWatermark("event_ts", DEDUP_WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
        .writeStream.queryName("clean_lake")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/clean_lake")
        .partitionBy("event_date", "event_type")
        .trigger(processingTime="30 seconds")  # fewer, larger files
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

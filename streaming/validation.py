"""Contract validation and Dead Letter Queue routing.

Pure DataFrame -> DataFrame functions: they work the same on a streaming
DataFrame read from Kafka and on a static DataFrame built in a unit test.
"""

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from streaming.schemas import (
    ALLOWED_EVENT_TYPES,
    CONTRACT_FIELDS,
    CONTRACT_VERSION,
    CORRUPT_RECORD_COLUMN,
    PARSE_SCHEMA,
    REQUIRED_FIELDS,
)

# Client clocks drift; events from the "future" beyond this tolerance are rejected.
MAX_CLOCK_SKEW = "5 minutes"

# Kafka source columns kept in the DLQ so a rejected event can be traced back
# to its exact position in the raw topic (and replayed after a fix).
KAFKA_METADATA_COLUMNS = ("topic", "partition", "offset", "timestamp")


def _field(name: str) -> Column:
    return F.col(f"data.{name}")


def validation_rules() -> list[tuple[Column, str]]:
    """Ordered (condition, error_code) pairs. The first matching rule wins."""
    all_contract_fields_null = reduce(
        lambda acc, cond: acc & cond, [_field(f).isNull() for f in CONTRACT_FIELDS]
    )
    corrupt = _field(CORRUPT_RECORD_COLUMN).isNotNull()

    rules = [
        # 1. Structural problems
        (F.col("data").isNull() | (corrupt & all_contract_fields_null), "MALFORMED_JSON"),
        (corrupt, "SCHEMA_TYPE_MISMATCH"),
    ]
    # 2. Contract: required fields
    rules += [(_field(f).isNull(), f"MISSING_REQUIRED_{f.upper()}") for f in REQUIRED_FIELDS]
    # 3. Contract: domain values
    rules += [
        (~_field("event_type").isin(*ALLOWED_EVENT_TYPES), "INVALID_EVENT_TYPE"),
        (F.col("event_ts").isNull(), "INVALID_TIMESTAMP"),
        (
            F.col("event_ts") > F.current_timestamp() + F.expr(f"INTERVAL {MAX_CLOCK_SKEW}"),
            "TIMESTAMP_IN_FUTURE",
        ),
        # 4. Deterministic business rules
        ((_field("event_type") == "purchase") & _field("price").isNull(), "MISSING_PRICE_FOR_PURCHASE"),
        (_field("price") < 0, "NEGATIVE_PRICE"),
    ]
    return rules


def _error_code_column() -> Column:
    rules = validation_rules()
    first_cond, first_code = rules[0]
    expr = F.when(first_cond, F.lit(first_code))
    for cond, code in rules[1:]:
        expr = expr.when(cond, F.lit(code))
    return expr  # NULL when no rule matched -> valid event


def validate_events(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split raw events into (valid_events, invalid_events).

    `raw_df` must have a `value` column (string or binary). Kafka metadata
    columns, if present, are propagated to the DLQ records.
    """
    metadata = [c for c in KAFKA_METADATA_COLUMNS if c in raw_df.columns]
    payload = F.col("value").cast("string")

    parsed = raw_df.select(
        payload.alias("raw_payload"),
        F.from_json(
            payload, PARSE_SCHEMA, {"columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN}
        ).alias("data"),
        *[F.col(c).alias(f"kafka_{c}") for c in metadata],
    ).withColumn(
        # try_to_timestamp returns NULL instead of failing the job under ANSI mode
        "event_ts",
        F.try_to_timestamp(_field("event_timestamp")),
    )

    validated = parsed.withColumn("validation_error", _error_code_column())

    valid_events = validated.filter(F.col("validation_error").isNull()).select(
        *[_field(f).alias(f) for f in CONTRACT_FIELDS],
        F.col("event_ts"),
        F.to_date("event_ts").alias("event_date"),
    )

    invalid_events = validated.filter(F.col("validation_error").isNotNull()).select(
        "raw_payload",
        "validation_error",
        *[f"kafka_{c}" for c in metadata],
        F.lit(CONTRACT_VERSION).alias("contract_version"),
        F.current_timestamp().alias("rejected_at"),
    )

    return valid_events, invalid_events

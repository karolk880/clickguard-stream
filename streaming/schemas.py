"""Clickstream event contract (v1).

This module is the single source of truth for what a valid clickstream event
looks like. Producers (frontend / backend SDKs) and the validation pipeline
should both depend on it.
"""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CONTRACT_VERSION = "1.0.0"

ALLOWED_EVENT_TYPES = ("view_offer", "add_to_cart", "purchase")

# NOTE: Spark's `from_json` ignores `nullable=False` and always returns nullable
# fields, so the nullability below is documentation only. Required fields are
# enforced explicitly in `streaming.validation`.
REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "event_timestamp")

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=False),
    ]
)

CONTRACT_FIELDS = tuple(field.name for field in CLICKSTREAM_SCHEMA.fields)

# Extra column populated by Spark with the raw payload whenever the JSON is
# malformed or a field cannot be cast to the declared type (e.g. price="free").
# Without it, a broken payload parses into a struct of NULLs and would be
# misreported as "missing field" instead of "malformed JSON".
CORRUPT_RECORD_COLUMN = "_corrupt_record"

PARSE_SCHEMA = StructType(
    CLICKSTREAM_SCHEMA.fields
    + [StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True)]
)

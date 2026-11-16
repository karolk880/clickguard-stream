import json
from datetime import date, datetime

import pytest

from streaming.validation import validate_events

BASE_EVENT = {
    "event_id": "e-1",
    "user_id": "u-1",
    "session_id": "s-1",
    "event_type": "purchase",
    "category": "electronics",
    "price": 199.99,
    "event_timestamp": "2026-11-16T12:00:00Z",
}


def make_event(drop: tuple[str, ...] = (), **overrides) -> str:
    event = {**BASE_EVENT, **overrides}
    for field in drop:
        event.pop(field)
    return json.dumps(event)


def run(spark, *payloads: str):
    raw_df = spark.createDataFrame([(p,) for p in payloads], ["value"])
    valid_df, invalid_df = validate_events(raw_df)
    return valid_df.collect(), invalid_df.collect()


def test_valid_event_passes_with_derived_columns(spark):
    valid, invalid = run(spark, make_event())

    assert invalid == []
    assert len(valid) == 1
    row = valid[0]
    assert row.event_id == "e-1"
    assert row.price == 199.99
    assert row.event_ts == datetime(2026, 11, 16, 12, 0, 0)
    assert row.event_date == date(2026, 11, 16)


def test_view_event_without_price_is_valid(spark):
    valid, invalid = run(spark, make_event(drop=("price",), event_type="view_offer"))
    assert len(valid) == 1 and invalid == []


@pytest.mark.parametrize(
    "payload, expected_error",
    [
        ("{broken_json", "MALFORMED_JSON"),
        ("", "MALFORMED_JSON"),
        ("[1, 2, 3]", "MALFORMED_JSON"),
        (make_event(price="free"), "SCHEMA_TYPE_MISMATCH"),
        (make_event(drop=("event_id",)), "MISSING_REQUIRED_EVENT_ID"),
        (make_event(drop=("user_id",)), "MISSING_REQUIRED_USER_ID"),
        (make_event(user_id=None), "MISSING_REQUIRED_USER_ID"),
        (make_event(drop=("event_type",)), "MISSING_REQUIRED_EVENT_TYPE"),
        (make_event(drop=("event_timestamp",)), "MISSING_REQUIRED_EVENT_TIMESTAMP"),
        (make_event(event_type="hover"), "INVALID_EVENT_TYPE"),
        (make_event(event_timestamp="yesterday"), "INVALID_TIMESTAMP"),
        (make_event(event_timestamp="2099-01-01T00:00:00Z"), "TIMESTAMP_IN_FUTURE"),
        (make_event(drop=("price",)), "MISSING_PRICE_FOR_PURCHASE"),
        (make_event(price=-10.0), "NEGATIVE_PRICE"),
    ],
)
def test_contract_violations_are_routed_to_dlq(spark, payload, expected_error):
    valid, invalid = run(spark, payload)

    assert valid == []
    assert len(invalid) == 1
    assert invalid[0].validation_error == expected_error
    assert invalid[0].raw_payload == payload
    assert invalid[0].contract_version == "1.0.0"


def test_first_matching_rule_wins(spark):
    # Both missing user_id and a negative price - the contract error is reported.
    _, invalid = run(spark, make_event(drop=("user_id",), price=-1.0))
    assert invalid[0].validation_error == "MISSING_REQUIRED_USER_ID"


def test_mixed_batch_routing(spark):
    valid, invalid = run(
        spark,
        make_event(event_id="ok-1"),
        make_event(event_id="ok-2", event_type="view_offer"),
        make_event(drop=("user_id",)),
        make_event(price=-10.0),
        "{broken_json",
    )

    assert sorted(r.event_id for r in valid) == ["ok-1", "ok-2"]
    assert sorted(r.validation_error for r in invalid) == [
        "MALFORMED_JSON",
        "MISSING_REQUIRED_USER_ID",
        "NEGATIVE_PRICE",
    ]


def test_kafka_metadata_is_kept_in_dlq(spark):
    raw_df = spark.createDataFrame(
        [(b"{broken_json", "clickstream-raw", 2, 1234)],
        ["value", "topic", "partition", "offset"],
    )
    valid_df, invalid_df = validate_events(raw_df)

    assert valid_df.count() == 0
    row = invalid_df.collect()[0]
    assert (row.kafka_topic, row.kafka_partition, row.kafka_offset) == ("clickstream-raw", 2, 1234)
    assert row.raw_payload == "{broken_json"

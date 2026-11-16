"""Contract test: every fault the producer injects is classified as expected."""

import random

import pytest

from producer.clickstream_producer import FAULTS, apply_fault, build_event
from streaming.validation import validate_events

EXPECTED = {
    "malformed_json": "MALFORMED_JSON",
    "missing_user_id": "MISSING_REQUIRED_USER_ID",
    "type_mismatch": "SCHEMA_TYPE_MISMATCH",
    "unknown_event_type": "INVALID_EVENT_TYPE",
    "negative_price": "NEGATIVE_PRICE",
    "late_event": None,  # late but valid
    None: None,  # clean event
}


def test_every_fault_has_an_expectation():
    assert {name for name, _ in FAULTS} <= EXPECTED.keys()


@pytest.mark.parametrize("fault", list(EXPECTED))
def test_producer_fault_is_classified(spark, fault):
    payload = apply_fault(build_event(random.Random(42)), fault)
    valid_df, invalid_df = validate_events(spark.createDataFrame([(payload,)], ["value"]))

    errors = [r.validation_error for r in invalid_df.collect()]
    if EXPECTED[fault] is None:
        assert errors == [] and valid_df.count() == 1
    else:
        assert errors == [EXPECTED[fault]]

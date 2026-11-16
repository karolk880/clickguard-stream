import os
import time

import pytest
from pyspark.sql import SparkSession

# PySpark converts timestamps to the *Python process* local time on collect().
# Pin it to UTC so tests behave the same on a laptop in Warsaw and in CI.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("clickguard-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

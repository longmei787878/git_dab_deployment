from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from booking_enrichment import build_enriched


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("booking-enrichment-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_enriched_preserves_bookings_and_enriches_matches(spark: SparkSession):
    bookings = spark.createDataFrame(
        [
            ("B1", "P1", "F1", "A1", Decimal("125.50"), date(2025, 1, 1)),
            ("B2", "P2", "F2", "UNKNOWN", Decimal("99.00"), date(2025, 1, 2)),
        ],
        "booking_id string, passenger_id string, flight_id string, airport_id string, "
        "amount decimal(10,2), booking_date date",
    )
    passengers = spark.createDataFrame(
        [
            ("P1", "Alice", "F", "US"),
            ("P1", "Alice duplicate", "F", "US"),
            ("P2", "Bob", "M", "CA"),
        ],
        "passenger_id string, name string, gender string, nationality string",
    )
    airports = spark.createDataFrame(
        [("A1", "Test Airport", "Test City", "US")],
        "airport_id string, airport_name string, city string, country string",
    )

    rows = {
        row.booking_id: row
        for row in build_enriched(bookings, passengers, airports).collect()
    }

    assert set(rows) == {"B1", "B2"}
    assert rows["B1"].passenger_name == "Alice"
    assert rows["B1"].airport_name == "Test Airport"
    assert rows["B2"].airport_name is None

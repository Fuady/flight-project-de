"""
tests/test_spark_transform.py
────────────────────────────────────────────────────────────────────────────
Unit tests for spark/spark_transform.py.
Uses a local SparkSession (no Dataproc required).
Run: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
from spark_transform import transform


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("FlightsTest")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def make_raw_df(spark, rows):
    """Helper: create a raw-schema DataFrame from a list of dicts."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("FlightDate",          StringType(), True),
        StructField("Reporting_Airline",   StringType(), True),
        StructField("Flight_Number_Reporting_Airline", StringType(), True),
        StructField("Origin",              StringType(), True),
        StructField("Dest",               StringType(), True),
        StructField("DepDelay",            StringType(), True),
        StructField("ArrDelay",            StringType(), True),
        StructField("Cancelled",           StringType(), True),
        StructField("CancellationCode",    StringType(), True),
        StructField("CarrierDelay",        StringType(), True),
        StructField("WeatherDelay",        StringType(), True),
        StructField("NASDelay",            StringType(), True),
        StructField("SecurityDelay",       StringType(), True),
        StructField("LateAircraftDelay",   StringType(), True),
        StructField("ActualElapsedTime",   StringType(), True),
        StructField("Distance",            StringType(), True),
        StructField("DepTime",             StringType(), True),
        StructField("ArrTime",             StringType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)


def base_row(**overrides):
    defaults = {
        "FlightDate": "2024-01-15",
        "Reporting_Airline": "AA",
        "Flight_Number_Reporting_Airline": "100",
        "Origin": "LAX",
        "Dest": "JFK",
        "DepDelay": "25.0",
        "ArrDelay": "30.0",
        "Cancelled": "0.0",
        "CancellationCode": None,
        "CarrierDelay": "10.0",
        "WeatherDelay": "5.0",
        "NASDelay": "15.0",
        "SecurityDelay": "0.0",
        "LateAircraftDelay": "0.0",
        "ActualElapsedTime": "320.0",
        "Distance": "2475.0",
        "DepTime": "0800",
        "ArrTime": "1600",
    }
    return {**defaults, **overrides}


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestColumnRenaming:
    def test_output_has_snake_case_columns(self, spark):
        df = make_raw_df(spark, [base_row()])
        result = transform(df)
        cols = result.columns
        assert "fl_date"    in cols
        assert "mkt_carrier" in cols
        assert "origin"     in cols
        assert "dest"       in cols
        assert "arr_delay"  in cols
        assert "dep_delay"  in cols

    def test_no_raw_columns_remain(self, spark):
        df = make_raw_df(spark, [base_row()])
        result = transform(df)
        raw_cols = [c for c in result.columns if c.endswith("_raw")]
        assert raw_cols == [], f"Raw columns still present: {raw_cols}"


class TestTypeCasting:
    def test_arr_delay_is_float(self, spark):
        df = make_raw_df(spark, [base_row(ArrDelay="45.5")])
        result = transform(df)
        dtype = dict(result.dtypes)["arr_delay"]
        assert dtype == "float", f"Expected float, got {dtype}"

    def test_fl_date_is_date(self, spark):
        df = make_raw_df(spark, [base_row()])
        result = transform(df)
        dtype = dict(result.dtypes)["fl_date"]
        assert dtype == "date", f"Expected date, got {dtype}"

    def test_flight_number_is_int(self, spark):
        df = make_raw_df(spark, [base_row()])
        result = transform(df)
        dtype = dict(result.dtypes)["mkt_carrier_fl_num"]
        assert dtype == "int", f"Expected int, got {dtype}"


class TestNullFiltering:
    def test_drops_row_with_null_fl_date(self, spark):
        df = make_raw_df(spark, [base_row(FlightDate=None)])
        result = transform(df)
        assert result.count() == 0

    def test_drops_row_with_null_origin(self, spark):
        df = make_raw_df(spark, [base_row(Origin=None)])
        result = transform(df)
        assert result.count() == 0

    def test_keeps_valid_row(self, spark):
        df = make_raw_df(spark, [base_row()])
        result = transform(df)
        assert result.count() == 1


class TestDerivedColumns:
    def test_is_delayed_true_when_arr_delay_over_15(self, spark):
        df = make_raw_df(spark, [base_row(ArrDelay="30.0")])
        result = transform(df)
        row = result.first()
        assert row["is_delayed"] is True

    def test_is_delayed_false_when_arr_delay_under_15(self, spark):
        df = make_raw_df(spark, [base_row(ArrDelay="5.0")])
        result = transform(df)
        row = result.first()
        assert row["is_delayed"] is False

    def test_delay_bucket_on_time(self, spark):
        df = make_raw_df(spark, [base_row(ArrDelay="-5.0")])
        result = transform(df)
        assert result.first()["delay_bucket"] == "On time"

    def test_delay_bucket_moderate(self, spark):
        df = make_raw_df(spark, [base_row(ArrDelay="45.0")])
        result = transform(df)
        assert result.first()["delay_bucket"] == "Moderate (16-60 min)"

    def test_delay_bucket_extreme(self, spark):
        df = make_raw_df(spark, [base_row(ArrDelay="200.0")])
        result = transform(df)
        assert result.first()["delay_bucket"] == "Severe (1-3 hrs)"

    def test_year_month_extracted(self, spark):
        df = make_raw_df(spark, [base_row(FlightDate="2024-03-22")])
        result = transform(df)
        row = result.first()
        assert row["year"]  == 2024
        assert row["month"] == 3

    def test_total_delay_defaults_to_zero_when_null(self, spark):
        df = make_raw_df(spark, [base_row(ArrDelay=None)])
        result = transform(df)
        row = result.first()
        assert row["total_delay"] == 0.0

    def test_primary_delay_cause_carrier(self, spark):
        df = make_raw_df(spark, [base_row(
            CarrierDelay="50.0",
            WeatherDelay="5.0",
            NASDelay="0.0",
            LateAircraftDelay="0.0",
        )])
        result = transform(df)
        assert result.first()["primary_delay_cause"] == "Carrier"

    def test_primary_delay_cause_weather(self, spark):
        df = make_raw_df(spark, [base_row(
            CarrierDelay="5.0",
            WeatherDelay="80.0",
            NASDelay="10.0",
            LateAircraftDelay="0.0",
        )])
        result = transform(df)
        assert result.first()["primary_delay_cause"] == "Weather"


class TestEdgeCases:
    def test_multiple_rows(self, spark):
        rows = [base_row(ArrDelay=str(d)) for d in [10, 30, -5, 90, 200]]
        df = make_raw_df(spark, rows)
        result = transform(df)
        assert result.count() == 5

    def test_all_null_delays_still_passes(self, spark):
        df = make_raw_df(spark, [base_row(
            ArrDelay=None,
            DepDelay=None,
            CarrierDelay=None,
            WeatherDelay=None,
        )])
        result = transform(df)
        assert result.count() == 1

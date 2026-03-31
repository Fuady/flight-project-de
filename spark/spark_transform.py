"""
spark_transform.py
────────────────────────────────────────────────────────────────────────────
PySpark job that cleans and transforms raw BTS CSV data from GCS,
then writes optimised Parquet files back to GCS processed bucket.

Transformations applied:
  - Column renaming (snake_case)
  - Type casting (dates, floats, ints)
  - Null filtering (drop rows with no fl_date, origin, or dest)
  - Delay flag columns added (is_delayed, delay_bucket)
  - Derived columns (total_delay, delay_cause)
  - Cancelled flight separation

Usage (Dataproc):
  spark-submit spark_transform.py \
    --input  gs://PROJECT-flights-raw/raw/2024/01/flights.csv \
    --output gs://PROJECT-flights-processed/processed/2024/01/
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, FloatType, IntegerType, StringType, StructField, StructType
)

# ─── Schema ───────────────────────────────────────────────────────────────────

RAW_SCHEMA = StructType([
    StructField("FlightDate",          StringType(),  True),
    StructField("Reporting_Airline",   StringType(),  True),
    StructField("Flight_Number_Reporting_Airline", StringType(), True),
    StructField("Origin",              StringType(),  True),
    StructField("Dest",                StringType(),  True),
    StructField("DepDelay",            StringType(),  True),
    StructField("ArrDelay",            StringType(),  True),
    StructField("Cancelled",           StringType(),  True),
    StructField("CancellationCode",    StringType(),  True),
    StructField("CarrierDelay",        StringType(),  True),
    StructField("WeatherDelay",        StringType(),  True),
    StructField("NASDelay",            StringType(),  True),
    StructField("SecurityDelay",       StringType(),  True),
    StructField("LateAircraftDelay",   StringType(),  True),
    StructField("ActualElapsedTime",   StringType(),  True),
    StructField("Distance",            StringType(),  True),
    StructField("DepTime",             StringType(),  True),
    StructField("ArrTime",             StringType(),  True),
])


def parse_args():
    parser = argparse.ArgumentParser(description="Flights DE Spark Transform")
    parser.add_argument("--input",  required=True, help="GCS path to raw CSV")
    parser.add_argument("--output", required=True, help="GCS path for Parquet output")
    return parser.parse_args()


def build_spark():
    return (
        SparkSession.builder
        .appName("FlightsDelayTransform")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, input_path: str):
    return (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .schema(RAW_SCHEMA)
        .csv(input_path)
    )


def transform(df):
    """Apply all cleaning and enrichment transformations."""

    df = (
        df
        # ── Rename columns to snake_case ──────────────────────────────────
        .withColumnRenamed("FlightDate",          "fl_date_raw")
        .withColumnRenamed("Reporting_Airline",   "mkt_carrier")
        .withColumnRenamed("Flight_Number_Reporting_Airline", "mkt_carrier_fl_num")
        .withColumnRenamed("Origin",              "origin")
        .withColumnRenamed("Dest",                "dest")
        .withColumnRenamed("DepDelay",            "dep_delay_raw")
        .withColumnRenamed("ArrDelay",            "arr_delay_raw")
        .withColumnRenamed("Cancelled",           "cancelled_raw")
        .withColumnRenamed("CancellationCode",    "cancellation_code")
        .withColumnRenamed("CarrierDelay",        "carrier_delay_raw")
        .withColumnRenamed("WeatherDelay",        "weather_delay_raw")
        .withColumnRenamed("NASDelay",            "nas_delay_raw")
        .withColumnRenamed("SecurityDelay",       "security_delay_raw")
        .withColumnRenamed("LateAircraftDelay",   "late_aircraft_delay_raw")
        .withColumnRenamed("ActualElapsedTime",   "actual_elapsed_time_raw")
        .withColumnRenamed("Distance",            "distance_raw")
        .withColumnRenamed("DepTime",             "dep_time")
        .withColumnRenamed("ArrTime",             "arr_time")
    )

    # ── Type casts ────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("fl_date",              F.to_date("fl_date_raw", "yyyy-MM-dd"))
        .withColumn("mkt_carrier_fl_num",   F.col("mkt_carrier_fl_num").cast(IntegerType()))
        .withColumn("dep_delay",            F.col("dep_delay_raw").cast(FloatType()))
        .withColumn("arr_delay",            F.col("arr_delay_raw").cast(FloatType()))
        .withColumn("cancelled",            F.col("cancelled_raw").cast(FloatType()))
        .withColumn("carrier_delay",        F.col("carrier_delay_raw").cast(FloatType()))
        .withColumn("weather_delay",        F.col("weather_delay_raw").cast(FloatType()))
        .withColumn("nas_delay",            F.col("nas_delay_raw").cast(FloatType()))
        .withColumn("security_delay",       F.col("security_delay_raw").cast(FloatType()))
        .withColumn("late_aircraft_delay",  F.col("late_aircraft_delay_raw").cast(FloatType()))
        .withColumn("actual_elapsed_time",  F.col("actual_elapsed_time_raw").cast(FloatType()))
        .withColumn("distance",             F.col("distance_raw").cast(FloatType()))
    )

    # ── Drop raw string columns ───────────────────────────────────────────
    raw_cols = [c for c in df.columns if c.endswith("_raw")]
    df = df.drop(*raw_cols)

    # ── Null filtering: require essential fields ──────────────────────────
    df = df.filter(
        F.col("fl_date").isNotNull()
        & F.col("origin").isNotNull()
        & F.col("dest").isNotNull()
        & F.col("mkt_carrier").isNotNull()
    )

    # ── Derived columns ───────────────────────────────────────────────────
    df = (
        df
        # Total delay minutes (arrival-based, defaulting to 0 if null)
        .withColumn(
            "total_delay",
            F.coalesce(F.col("arr_delay"), F.lit(0.0))
        )
        # Boolean: was the flight delayed > 15 min?
        .withColumn(
            "is_delayed",
            F.when(F.col("arr_delay") > 15, True).otherwise(False)
        )
        # Delay bucket for categorical analysis
        .withColumn(
            "delay_bucket",
            F.when(F.col("arr_delay") <= 0,   "On time")
             .when(F.col("arr_delay") <= 15,   "Minor (1-15 min)")
             .when(F.col("arr_delay") <= 60,   "Moderate (16-60 min)")
             .when(F.col("arr_delay") <= 180,  "Severe (1-3 hrs)")
             .otherwise("Extreme (>3 hrs)")
        )
        # Primary delay cause (the largest contributing factor)
        .withColumn(
            "primary_delay_cause",
            F.when(
                F.col("carrier_delay") == F.greatest(
                    F.coalesce("carrier_delay",       F.lit(0)),
                    F.coalesce("weather_delay",       F.lit(0)),
                    F.coalesce("nas_delay",           F.lit(0)),
                    F.coalesce("security_delay",      F.lit(0)),
                    F.coalesce("late_aircraft_delay", F.lit(0)),
                ), "Carrier"
            ).when(
                F.col("weather_delay") == F.greatest(
                    F.coalesce("carrier_delay",       F.lit(0)),
                    F.coalesce("weather_delay",       F.lit(0)),
                    F.coalesce("nas_delay",           F.lit(0)),
                    F.coalesce("security_delay",      F.lit(0)),
                    F.coalesce("late_aircraft_delay", F.lit(0)),
                ), "Weather"
            ).when(
                F.col("nas_delay") == F.greatest(
                    F.coalesce("carrier_delay",       F.lit(0)),
                    F.coalesce("weather_delay",       F.lit(0)),
                    F.coalesce("nas_delay",           F.lit(0)),
                    F.coalesce("security_delay",      F.lit(0)),
                    F.coalesce("late_aircraft_delay", F.lit(0)),
                ), "NAS"
            ).when(
                F.col("late_aircraft_delay") == F.greatest(
                    F.coalesce("carrier_delay",       F.lit(0)),
                    F.coalesce("weather_delay",       F.lit(0)),
                    F.coalesce("nas_delay",           F.lit(0)),
                    F.coalesce("security_delay",      F.lit(0)),
                    F.coalesce("late_aircraft_delay", F.lit(0)),
                ), "Late Aircraft"
            ).otherwise("Unknown")
        )
        # Date parts for partitioning and grouping
        .withColumn("year",  F.year("fl_date"))
        .withColumn("month", F.month("fl_date"))
    )

    return df


def write_parquet(df, output_path: str) -> None:
    """Write cleaned data as snappy-compressed Parquet, partitioned by year/month."""
    (
        df
        .repartition(10)
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)
    )
    print(f"Written to {output_path}")


def main():
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading from: {args.input}")
    raw_df = read_raw(spark, args.input)

    print("Raw record count:", raw_df.count())

    clean_df = transform(raw_df)

    print("Clean record count:", clean_df.count())
    clean_df.printSchema()
    clean_df.show(5, truncate=False)

    write_parquet(clean_df, args.output)

    spark.stop()
    print("Spark job complete.")


if __name__ == "__main__":
    main()

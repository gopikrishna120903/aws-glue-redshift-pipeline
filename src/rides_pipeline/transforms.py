"""PySpark transformations used by the Glue job. Pure functions: DataFrame in, DataFrame out.

Keeping the logic here (not in the Glue entry point) means it runs and is
tested on a laptop with plain PySpark, then runs unchanged inside AWS Glue.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

RAW_SCHEMA = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("driver_id", T.StringType()),
    T.StructField("rider_id", T.StringType()),
    T.StructField("product", T.StringType()),
    T.StructField("pickup_ts", T.StringType()),
    T.StructField("dropoff_ts", T.StringType()),
    T.StructField("pickup_zone_id", T.StringType()),
    T.StructField("dropoff_zone_id", T.StringType()),
    T.StructField("distance_miles", T.StringType()),
    T.StructField("fare_amount", T.StringType()),
    T.StructField("surge_multiplier", T.DoubleType()),
    T.StructField("tip_amount", T.StringType()),
    T.StructField("payment_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
])

MAX_TRIP_MILES = 150.0
MAX_TRIP_MINUTES = 240.0
MAX_SPEED_MPH = 80.0


def parse(raw: DataFrame) -> DataFrame:
    """Cast the string fields a JSON producer sends into proper types."""
    ts = lambda c: F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss'Z'")  # noqa: E731
    return raw.select(
        F.col("trip_id"),
        F.col("driver_id"),
        F.col("rider_id"),
        F.lower(F.trim("product")).alias("product"),
        ts("pickup_ts").alias("pickup_ts"),
        ts("dropoff_ts").alias("dropoff_ts"),
        F.col("pickup_zone_id").cast("int").alias("pickup_zone_id"),
        F.col("dropoff_zone_id").cast("int").alias("dropoff_zone_id"),
        F.col("distance_miles").cast("double").alias("distance_miles"),
        F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
        F.coalesce(F.col("surge_multiplier"), F.lit(1.0)).alias("surge_multiplier"),
        F.coalesce(F.col("tip_amount").cast("decimal(10,2)"), F.lit(0).cast("decimal(10,2)")).alias("tip_amount"),
        F.lower(F.col("payment_type")).alias("payment_type"),
        ts("event_ts").alias("event_ts"),
        F.input_file_name().alias("source_file"),
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the latest version of each trip (highest event_ts), so re-sent trips win."""
    w = Window.partitionBy("trip_id").orderBy(F.col("event_ts").desc_nulls_last(), F.col("source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def validate(df: DataFrame, zones: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split records into (valid, rejected). Rejected rows carry every rule they broke."""
    zone_ids = zones.select(F.col("zone_id").alias("_zid"))
    checked = (
        df.join(zone_ids.withColumnRenamed("_zid", "_pu"), F.col("pickup_zone_id") == F.col("_pu"), "left")
          .join(zone_ids.withColumnRenamed("_zid", "_do"), F.col("dropoff_zone_id") == F.col("_do"), "left")
    )
    minutes = (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
    rules = [
        ("missing_trip_id", F.col("trip_id").isNull()),
        ("missing_timestamps", F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull()),
        ("dropoff_before_pickup", F.col("dropoff_ts") <= F.col("pickup_ts")),
        ("trip_too_long", minutes > MAX_TRIP_MINUTES),
        ("invalid_distance", F.col("distance_miles").isNull() | (F.col("distance_miles") <= 0)
         | (F.col("distance_miles") > MAX_TRIP_MILES)),
        ("negative_amount", (F.col("fare_amount") < 0) | (F.col("tip_amount") < 0)),
        ("unknown_zone", F.col("_pu").isNull() | F.col("_do").isNull()),
    ]
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules]))
    checked = checked.withColumn("reject_reasons", reasons).drop("_pu", "_do")
    valid = checked.filter(F.size("reject_reasons") == 0).drop("reject_reasons")
    rejected = checked.filter(F.size("reject_reasons") > 0)
    return valid, rejected


def enrich(valid: DataFrame, zones: DataFrame) -> DataFrame:
    """Derive trip metrics and partition columns, and attach zone names."""
    pu = zones.select(F.col("zone_id").alias("pickup_zone_id"), F.col("zone_name").alias("pickup_zone"),
                      F.col("borough").alias("pickup_borough"))
    do = zones.select(F.col("zone_id").alias("dropoff_zone_id"), F.col("zone_name").alias("dropoff_zone"))
    minutes = (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
    out = (
        valid.join(F.broadcast(pu), "pickup_zone_id", "left")
             .join(F.broadcast(do), "dropoff_zone_id", "left")
             .withColumn("duration_minutes", F.round(minutes, 2))
             .withColumn("avg_speed_mph", F.round(F.col("distance_miles") / (minutes / 60.0), 2))
             .withColumn("total_amount", (F.col("fare_amount") + F.col("tip_amount")).cast("decimal(10,2)"))
             .withColumn("tip_pct", F.round((F.col("tip_amount") / F.when(F.col("fare_amount") > 0, F.col("fare_amount"))).cast("double"), 4))
             .withColumn("is_airport_trip", (F.col("pickup_zone") == "Airport") | (F.col("dropoff_zone") == "Airport"))
             .withColumn("is_surge", F.col("surge_multiplier") > 1.0)
             .withColumn("pickup_date", F.to_date("pickup_ts"))
             .withColumn("pickup_hour", F.hour("pickup_ts"))
    )
    # speed check needs duration, so it runs after enrichment; flagged rather than dropped
    out = out.withColumn("is_speed_outlier", F.col("avg_speed_mph") > MAX_SPEED_MPH)
    return out.select(
        "trip_id", "driver_id", "rider_id", "product", "pickup_ts", "dropoff_ts", "pickup_date", "pickup_hour",
        "pickup_zone_id", "pickup_zone", "pickup_borough", "dropoff_zone_id", "dropoff_zone",
        "distance_miles", "duration_minutes", "avg_speed_mph", "fare_amount", "tip_amount", "total_amount",
        "tip_pct", "surge_multiplier", "is_surge", "is_airport_trip", "is_speed_outlier", "payment_type",
        "event_ts", "source_file",
    )


def daily_zone_metrics(trips: DataFrame) -> DataFrame:
    """Aggregate for the Redshift reporting table: one row per day, pickup zone, and product."""
    return (
        trips.groupBy("pickup_date", "pickup_zone_id", "pickup_zone", "product")
             .agg(
                 F.count("*").alias("trips"),
                 F.countDistinct("driver_id").alias("active_drivers"),
                 F.round(F.sum("distance_miles"), 2).alias("total_miles"),
                 F.sum("fare_amount").cast("decimal(14,2)").alias("fare_revenue"),
                 F.sum("tip_amount").cast("decimal(14,2)").alias("tips"),
                 F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
                 F.round(F.avg(F.col("is_surge").cast("int")), 4).alias("surge_share"),
             )
    )


def run(spark: SparkSession, source_paths: list[str], zones_path: str, curated_path: str,
        rejects_path: str, metrics_path: str) -> dict:
    """Full transform: read landing JSON, write curated/rejects/metrics Parquet. Returns run stats."""
    zones = spark.read.option("header", True).csv(zones_path).select(
        F.col("zone_id").cast("int").alias("zone_id"), "zone_name", "borough")
    raw = spark.read.schema(RAW_SCHEMA).json(source_paths)
    parsed = parse(raw).cache()
    deduped = deduplicate(parsed)
    valid, rejected = validate(deduped, zones)
    trips = enrich(valid, zones).cache()

    # dynamic partition overwrite: re-running a day replaces only that day's partitions (idempotent)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # standard Parquet timestamps (not legacy INT96) so Redshift COPY and Athena read them cleanly
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    trips.write.mode("overwrite").partitionBy("pickup_date").parquet(curated_path)
    (rejected.withColumn("ingest_date", F.to_date("event_ts"))
             .write.mode("overwrite").partitionBy("ingest_date").parquet(rejects_path))
    daily_zone_metrics(trips).write.mode("overwrite").partitionBy("pickup_date").parquet(metrics_path)

    stats = {
        "records_read": parsed.count(),
        "after_dedup": deduped.count(),
        "valid": trips.count(),
        "rejected": rejected.count(),
    }
    stats["duplicates_removed"] = stats["records_read"] - stats["after_dedup"]
    parsed.unpersist()
    trips.unpersist()
    return stats

"""Run the transform locally on generated data (no AWS account needed).

    python -m rides_pipeline.local_runner
"""
from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from rides_pipeline.generate_data import generate, write_zone_lookup
from rides_pipeline.transforms import run

ROOT = Path(__file__).resolve().parents[2]


def local_spark() -> SparkSession:
    return (SparkSession.builder.master("local[2]").appName("rides-pipeline-local")
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


def main() -> None:
    data = ROOT / "data"
    landing, zones = data / "landing" / "trips", data / "reference" / "zones.csv"
    if not landing.exists():
        print("generated:", generate(landing))
    write_zone_lookup(zones)
    out = data / "output"
    shutil.rmtree(out, ignore_errors=True)
    spark = local_spark()
    spark.sparkContext.setLogLevel("ERROR")
    stats = run(spark, [str(landing)], str(zones), str(out / "curated" / "trips"),
                str(out / "rejects" / "trips"), str(out / "curated" / "daily_zone_metrics"))
    print("run stats:", stats)
    rejects = spark.read.parquet(str(out / "rejects" / "trips"))
    from pyspark.sql import functions as F
    print("reject reasons:")
    for row in rejects.select(F.explode("reject_reasons").alias("r")).groupBy("r").count().orderBy("r").collect():
        print(f"  {row['r']:<24}{row['count']:>6}")
    spark.stop()


if __name__ == "__main__":
    main()

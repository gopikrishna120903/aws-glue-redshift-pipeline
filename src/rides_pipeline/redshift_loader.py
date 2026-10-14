"""Load curated Parquet from S3 into Amazon Redshift with COPY + MERGE.

Statements run through the Redshift Data API (no JDBC driver or open port needed),
which is also what the Step Functions state machine calls.

Load pattern per table:
  1. COPY the run's Parquet partitions into a staging table
  2. MERGE staging into the target on the business key (upsert, so re-runs don't duplicate)
  3. TRUNCATE staging
"""
from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class TableLoad:
    target: str
    staging: str
    key_columns: tuple[str, ...]
    columns: tuple[str, ...]
    s3_prefix: str


TRIP_COLUMNS = (
    "trip_id", "driver_id", "rider_id", "product", "pickup_ts", "dropoff_ts", "pickup_hour",
    "pickup_zone_id", "pickup_zone", "pickup_borough", "dropoff_zone_id", "dropoff_zone",
    "distance_miles", "duration_minutes", "avg_speed_mph", "fare_amount", "tip_amount", "total_amount",
    "tip_pct", "surge_multiplier", "is_surge", "is_airport_trip", "is_speed_outlier", "payment_type",
    "event_ts", "source_file",
)
METRIC_COLUMNS = ("pickup_zone_id", "pickup_zone", "product", "trips", "active_drivers", "total_miles",
                  "fare_revenue", "tips", "avg_duration_minutes", "surge_share")


def copy_sql(load: TableLoad, iam_role_arn: str, partition_dates: list[str]) -> list[str]:
    """One COPY per date partition. pickup_date lives in the S3 path (Hive partition), so it's added after."""
    stmts = [f"TRUNCATE {load.staging};"]
    for d in partition_dates:
        cols = ", ".join(load.columns)
        stmts.append(
            f"COPY {load.staging} ({cols})\n"
            f"FROM '{load.s3_prefix.rstrip('/')}/pickup_date={d}/'\n"
            f"IAM_ROLE '{iam_role_arn}'\n"
            f"FORMAT AS PARQUET;"
        )
        stmts.append(f"UPDATE {load.staging} SET pickup_date = DATE '{d}' WHERE pickup_date IS NULL;")
    return stmts


def merge_sql(load: TableLoad) -> str:
    """Upsert staging into the target on the business key (Redshift MERGE)."""
    t = load.target
    on = " AND ".join(f"{t}.{k} = s.{k}" for k in load.key_columns)
    all_cols = list(dict.fromkeys((*load.columns, "pickup_date")))
    set_clause = ",\n    ".join(f"{c} = s.{c}" for c in all_cols if c not in load.key_columns)
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"s.{c}" for c in all_cols)
    return (
        f"MERGE INTO {t}\nUSING {load.staging} s\nON {on}\n"
        f"WHEN MATCHED THEN UPDATE SET\n    {set_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols})\nVALUES ({insert_vals});"
    )


def build_statements(iam_role_arn: str, curated_prefix: str, metrics_prefix: str, partition_dates: list[str]) -> list[str]:
    trips = TableLoad("analytics.fact_trips", "staging.trips", ("trip_id",), TRIP_COLUMNS, curated_prefix)
    metrics = TableLoad("analytics.daily_zone_metrics", "staging.daily_zone_metrics",
                        ("pickup_date", "pickup_zone_id", "product"), METRIC_COLUMNS, metrics_prefix)
    stmts: list[str] = []
    for load in (trips, metrics):
        stmts += copy_sql(load, iam_role_arn, partition_dates)
        stmts.append(merge_sql(load))
        stmts.append(f"TRUNCATE {load.staging};")
    return stmts


class RedshiftDataRunner:
    """Run statements as one transaction through the Redshift Data API and wait for the result."""

    def __init__(self, client, workgroup: str, database: str, poll_seconds: float = 2.0, timeout_seconds: float = 900):
        self.client = client
        self.workgroup = workgroup
        self.database = database
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds

    def run(self, statements: list[str]) -> str:
        resp = self.client.batch_execute_statement(
            WorkgroupName=self.workgroup, Database=self.database, Sqls=statements)
        statement_id = resp["Id"]
        waited = 0.0
        while True:
            desc = self.client.describe_statement(Id=statement_id)
            status = desc["Status"]
            if status == "FINISHED":
                return statement_id
            if status in ("FAILED", "ABORTED"):
                raise RuntimeError(f"Redshift load {status}: {desc.get('Error', 'no error message')}")
            if waited >= self.timeout_seconds:
                raise TimeoutError(f"Redshift load still {status} after {self.timeout_seconds}s")
            time.sleep(self.poll_seconds)
            waited += self.poll_seconds

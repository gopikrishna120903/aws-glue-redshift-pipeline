import pytest

from rides_pipeline.redshift_loader import TRIP_COLUMNS, RedshiftDataRunner, build_statements

ROLE = "arn:aws:iam::123456789012:role/rides-redshift-copy"


def stmts(dates=("2025-06-01", "2025-06-02")):
    return build_statements(ROLE, "s3://curated/curated/trips/", "s3://curated/curated/daily_zone_metrics/", list(dates))


def test_one_copy_per_partition_per_table():
    copies = [s for s in stmts() if s.startswith("COPY")]
    assert len(copies) == 4
    assert "FROM 's3://curated/curated/trips/pickup_date=2025-06-02/'" in copies[1]
    assert all("FORMAT AS PARQUET" in c and ROLE in c for c in copies)


def test_staging_is_truncated_before_and_after_each_load():
    s = stmts()
    assert s[0] == "TRUNCATE staging.trips;"
    merge_idx = next(i for i, x in enumerate(s) if x.startswith("MERGE INTO analytics.fact_trips"))
    assert s[merge_idx + 1] == "TRUNCATE staging.trips;"


def test_merge_upserts_on_business_keys():
    merges = [s for s in stmts() if s.startswith("MERGE")]
    assert "ON analytics.fact_trips.trip_id = s.trip_id" in merges[0]
    assert ("ON analytics.daily_zone_metrics.pickup_date = s.pickup_date AND "
            "analytics.daily_zone_metrics.pickup_zone_id = s.pickup_zone_id AND "
            "analytics.daily_zone_metrics.product = s.product") in merges[1]
    # the key is never updated, every other column is
    assert "trip_id = s.trip_id," not in merges[0].split("UPDATE SET")[1]
    for col in ("fare_amount", "pickup_date", "source_file"):
        assert f"{col} = s.{col}" in merges[0]


class FakeRedshiftData:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.sent = None

    def batch_execute_statement(self, **kwargs):
        self.sent = kwargs
        return {"Id": "stmt-1"}

    def describe_statement(self, Id):
        status = self.statuses.pop(0)
        return {"Status": status, "Error": "permission denied for relation fact_trips" if status == "FAILED" else ""}


def test_runner_waits_until_finished():
    client = FakeRedshiftData(["SUBMITTED", "STARTED", "FINISHED"])
    runner = RedshiftDataRunner(client, "rides-analytics", "analytics", poll_seconds=0)
    assert runner.run(["SELECT 1;"]) == "stmt-1"
    assert client.sent["WorkgroupName"] == "rides-analytics" and client.sent["Sqls"] == ["SELECT 1;"]


def test_runner_raises_with_redshift_error():
    runner = RedshiftDataRunner(FakeRedshiftData(["STARTED", "FAILED"]), "wg", "db", poll_seconds=0)
    with pytest.raises(RuntimeError, match="permission denied"):
        runner.run(["SELECT 1;"])


def test_parquet_column_order_matches_copy_list(spark, zones, tmp_path):
    # COPY maps Parquet columns by position, so the writer and loader must agree.
    from rides_pipeline.generate_data import generate, write_zone_lookup
    from rides_pipeline.transforms import run
    generate(tmp_path / "landing", days=1, trips_per_day=200)
    write_zone_lookup(tmp_path / "zones.csv")
    run(spark, [str(tmp_path / "landing")], str(tmp_path / "zones.csv"), str(tmp_path / "trips"),
        str(tmp_path / "rejects"), str(tmp_path / "metrics"))
    parquet_cols = [f.name for f in spark.read.parquet(str(tmp_path / "trips" / "pickup_date=2025-06-01")).schema]
    assert tuple(parquet_cols) == TRIP_COLUMNS

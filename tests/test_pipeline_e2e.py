"""End to end: files land in (mocked) S3, the manifest picks them up, Spark transforms them,
and a second run finds nothing new. Mirrors what the Glue job does, without AWS."""
import boto3
from moto import mock_aws

from rides_pipeline.generate_data import generate, write_zone_lookup
from rides_pipeline.manifest import S3Manifest
from rides_pipeline.transforms import run

BUCKET = "rides-landing-e2e"


def test_landing_to_curated_is_complete_and_idempotent(spark, tmp_path):
    local_landing = tmp_path / "generated"
    stats = generate(local_landing, days=2, trips_per_day=1500)
    write_zone_lookup(tmp_path / "zones.csv")

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        for f in local_landing.rglob("*.json"):
            s3.upload_file(str(f), BUCKET, "landing/trips/" + f.relative_to(local_landing).as_posix())

        manifest = S3Manifest(s3, BUCKET, "landing/trips/", "control/trips_manifest.json")
        pending = manifest.pending()
        assert len(pending) == stats["files"]

        # Glue reads s3:// paths directly; here we download the pending files and read them locally.
        staged = tmp_path / "staged"
        for f in pending:
            dest = staged / f.key
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(BUCKET, f.key, str(dest))

        out = tmp_path / "out"
        result = run(spark, [str(staged)], str(tmp_path / "zones.csv"), str(out / "trips"),
                     str(out / "rejects"), str(out / "metrics"))
        manifest.mark_processed(pending, run_id="e2e-1", stats=result)

        # every record is accounted for: duplicates removed, corrupt rows rejected, the rest curated
        assert result["records_read"] == stats["records"]
        assert result["duplicates_removed"] == stats["duplicates"]
        assert result["rejected"] == stats["corrupt"]
        assert result["valid"] == stats["unique_trips"] - stats["corrupt"]

        trips = spark.read.parquet(str(out / "trips"))
        assert trips.count() == result["valid"]
        assert trips.select("trip_id").distinct().count() == result["valid"]
        assert sorted(r.pickup_date.isoformat() for r in trips.select("pickup_date").distinct().collect()) == \
            ["2025-06-01", "2025-06-02"]

        # aggregate table agrees with the trip table
        metrics = spark.read.parquet(str(out / "metrics"))
        assert metrics.groupBy().sum("trips").first()[0] == result["valid"]

        # a second run finds nothing new
        assert manifest.pending() == []


def test_rerunning_a_day_replaces_its_partition(spark, tmp_path):
    landing = tmp_path / "landing"
    generate(landing, days=1, trips_per_day=300)
    write_zone_lookup(tmp_path / "zones.csv")
    args = (str(tmp_path / "zones.csv"), str(tmp_path / "trips"), str(tmp_path / "rejects"), str(tmp_path / "metrics"))
    first = run(spark, [str(landing)], *args)
    second = run(spark, [str(landing)], *args)
    assert first == second
    assert spark.read.parquet(str(tmp_path / "trips")).count() == first["valid"]  # no doubling

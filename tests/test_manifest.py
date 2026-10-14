import boto3
import pytest
from moto import mock_aws

from rides_pipeline.manifest import S3Manifest

BUCKET = "rides-landing-test"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def put(s3, key, body=b'{"trip_id": "T1"}\n'):
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)


def manifest(s3):
    return S3Manifest(s3, BUCKET, "landing/trips/", "control/trips_manifest.json")


def test_first_run_sees_every_file(s3):
    put(s3, "landing/trips/dt=2025-06-01/hour=08/part-0000.json")
    put(s3, "landing/trips/dt=2025-06-01/hour=09/part-0000.json")
    put(s3, "landing/trips/dt=2025-06-01/hour=09/_SUCCESS", b"")  # ignored: not JSON
    assert len(manifest(s3).pending()) == 2


def test_processed_files_are_not_picked_up_again(s3):
    put(s3, "landing/trips/dt=2025-06-01/hour=08/part-0000.json")
    m = manifest(s3)
    m.mark_processed(m.pending(), run_id="run-1")
    assert m.pending() == []
    put(s3, "landing/trips/dt=2025-06-01/hour=09/part-0000.json")
    assert [f.key for f in m.pending()] == ["landing/trips/dt=2025-06-01/hour=09/part-0000.json"]


def test_rewritten_file_is_reprocessed(s3):
    key = "landing/trips/dt=2025-06-01/hour=08/part-0000.json"
    put(s3, key)
    m = manifest(s3)
    m.mark_processed(m.pending(), run_id="run-1")
    put(s3, key, b'{"trip_id": "T1", "tip_amount": "3.00"}\n')  # producer re-sent the file
    assert [f.key for f in m.pending()] == [key]


def test_run_history_is_recorded(s3):
    put(s3, "landing/trips/dt=2025-06-01/hour=08/part-0000.json")
    m = manifest(s3)
    m.mark_processed(m.pending(), run_id="run-1", stats={"valid": 10})
    import json
    saved = json.loads(s3.get_object(Bucket=BUCKET, Key="control/trips_manifest.json")["Body"].read())
    assert saved["runs"][0]["run_id"] == "run-1" and saved["runs"][0]["stats"] == {"valid": 10}

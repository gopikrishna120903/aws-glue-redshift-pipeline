"""Lambda handler called by Step Functions to load Redshift after the Glue run.

Environment variables: REDSHIFT_WORKGROUP, REDSHIFT_DATABASE, REDSHIFT_COPY_ROLE_ARN,
CURATED_TRIPS_PREFIX, CURATED_METRICS_PREFIX.
"""
from __future__ import annotations

import json
import os

import boto3

from rides_pipeline.redshift_loader import RedshiftDataRunner, build_statements


def handler(event, _context):
    s3 = boto3.client("s3")
    manifest = json.loads(s3.get_object(Bucket=event["manifest_bucket"], Key=event["manifest_key"])["Body"].read())
    last_run = manifest["runs"][-1] if manifest["runs"] else {}
    dates = last_run.get("stats", {}).get("partition_dates", [])
    if not dates:
        return {"loaded_dates": [], "message": "nothing to load"}
    stmts = build_statements(
        iam_role_arn=os.environ["REDSHIFT_COPY_ROLE_ARN"],
        curated_prefix=os.environ["CURATED_TRIPS_PREFIX"],
        metrics_prefix=os.environ["CURATED_METRICS_PREFIX"],
        partition_dates=dates,
    )
    runner = RedshiftDataRunner(boto3.client("redshift-data"), os.environ["REDSHIFT_WORKGROUP"],
                                os.environ["REDSHIFT_DATABASE"], timeout_seconds=780)
    statement_id = runner.run(stmts)
    return {"loaded_dates": dates, "statement_id": statement_id, "statements": len(stmts)}

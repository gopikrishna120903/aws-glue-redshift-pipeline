"""AWS Glue entry point (Glue 5.0, Spark 3.5).

Job parameters:
  --landing_bucket, --landing_prefix, --manifest_key, --zones_path,
  --curated_path, --rejects_path, --metrics_path

Upload src/rides_pipeline as a zip with --extra-py-files so `transforms` and
`manifest` import inside Glue. The job writes the run's partition dates to
the manifest so the Step Functions Redshift step knows what to COPY.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone


def main() -> None:
    import boto3
    from awsglue.context import GlueContext  # available only inside AWS Glue
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    from rides_pipeline.manifest import S3Manifest
    from rides_pipeline.transforms import run

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "landing_bucket", "landing_prefix", "manifest_key", "zones_path",
        "curated_path", "rejects_path", "metrics_path",
    ])
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    manifest = S3Manifest(boto3.client("s3"), args["landing_bucket"], args["landing_prefix"], args["manifest_key"])
    pending = manifest.pending()
    if not pending:
        print("No new landing files; nothing to do.")
        job.commit()
        return

    paths = [f"s3://{args['landing_bucket']}/{f.key}" for f in pending]
    stats = run(spark, paths, args["zones_path"], args["curated_path"], args["rejects_path"], args["metrics_path"])
    stats["partition_dates"] = sorted({k.split("dt=")[1][:10] for k in (f.key for f in pending) if "dt=" in k})
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest.mark_processed(pending, run_id=f"{args['JOB_NAME']}-{run_id}", stats=stats)
    print(stats)
    job.commit()


if __name__ == "__main__":
    main()

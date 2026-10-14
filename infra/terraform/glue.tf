data "archive_file" "pipeline_lib" {
  type        = "zip"
  source_dir  = "${path.module}/../../src"
  output_path = "${path.module}/build/rides_pipeline.zip"
  excludes    = ["rides_pipeline/__pycache__"]
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "glue/glue_job.py"
  source = "${path.module}/../../src/rides_pipeline/glue_job.py"
  etag   = filemd5("${path.module}/../../src/rides_pipeline/glue_job.py")
}

resource "aws_s3_object" "glue_lib" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = "glue/rides_pipeline.zip"
  source = data.archive_file.pipeline_lib.output_path
  etag   = data.archive_file.pipeline_lib.output_md5
}

resource "aws_glue_catalog_database" "rides" {
  name = "${var.name_prefix}_curated"
}

resource "aws_glue_job" "transform" {
  name              = "${var.name_prefix}-transform"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_object.glue_script.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${aws_s3_object.glue_lib.bucket}/${aws_s3_object.glue_lib.key}"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--landing_bucket"                   = aws_s3_bucket.this["landing"].id
    "--landing_prefix"                   = "landing/trips/"
    "--manifest_key"                     = "control/trips_manifest.json"
    "--zones_path"                       = "s3://${aws_s3_bucket.this["landing"].id}/reference/zones.csv"
    "--curated_path"                     = "s3://${aws_s3_bucket.this["curated"].id}/curated/trips/"
    "--rejects_path"                     = "s3://${aws_s3_bucket.this["curated"].id}/rejects/trips/"
    "--metrics_path"                     = "s3://${aws_s3_bucket.this["curated"].id}/curated/daily_zone_metrics/"
  }

  execution_property {
    max_concurrent_runs = 1 # the manifest assumes one run at a time
  }
}

# Makes the curated Parquet queryable from Athena as well as Redshift Spectrum.
resource "aws_glue_crawler" "curated" {
  name          = "${var.name_prefix}-curated-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.rides.name

  s3_target {
    path = "s3://${aws_s3_bucket.this["curated"].id}/curated/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

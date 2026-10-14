data "archive_file" "loader" {
  type        = "zip"
  source_dir  = "${path.module}/../../src"
  output_path = "${path.module}/build/loader.zip"
  excludes    = ["rides_pipeline/__pycache__"]
}

resource "aws_lambda_function" "loader" {
  function_name    = "${var.name_prefix}-redshift-loader"
  role             = aws_iam_role.loader.arn
  runtime          = "python3.12"
  handler          = "rides_pipeline.lambda_loader.handler"
  filename         = data.archive_file.loader.output_path
  source_code_hash = data.archive_file.loader.output_base64sha256
  timeout          = 840

  environment {
    variables = {
      REDSHIFT_WORKGROUP     = aws_redshiftserverless_workgroup.this.workgroup_name
      REDSHIFT_DATABASE      = aws_redshiftserverless_namespace.this.db_name
      REDSHIFT_COPY_ROLE_ARN = aws_iam_role.redshift_copy.arn
      CURATED_TRIPS_PREFIX   = "s3://${aws_s3_bucket.this["curated"].id}/curated/trips/"
      CURATED_METRICS_PREFIX = "s3://${aws_s3_bucket.this["curated"].id}/curated/daily_zone_metrics/"
    }
  }
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.name_prefix}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = templatefile("${path.module}/../../stepfunctions/pipeline.asl.json", {
    glue_job_name        = aws_glue_job.transform.name
    loader_function_name = aws_lambda_function.loader.function_name
    landing_bucket       = aws_s3_bucket.this["landing"].id
    manifest_key         = "control/trips_manifest.json"
    redshift_workgroup   = aws_redshiftserverless_workgroup.this.workgroup_name
    redshift_database    = aws_redshiftserverless_namespace.this.db_name
    alerts_topic_arn     = aws_sns_topic.alerts.arn
  })
}

resource "aws_cloudwatch_event_rule" "schedule" {
  name                = "${var.name_prefix}-pipeline-schedule"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "pipeline" {
  rule     = aws_cloudwatch_event_rule.schedule.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.events.arn
}

output "landing_bucket" {
  value = aws_s3_bucket.this["landing"].id
}

output "curated_bucket" {
  value = aws_s3_bucket.this["curated"].id
}

output "glue_job_name" {
  value = aws_glue_job.transform.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.this.workgroup_name
}

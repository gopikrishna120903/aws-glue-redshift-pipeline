resource "aws_redshiftserverless_namespace" "this" {
  namespace_name        = "${var.name_prefix}-analytics"
  db_name               = "analytics"
  manage_admin_password = true # password stored in Secrets Manager, never in state as plain text
  admin_username        = "admin"
  iam_roles             = [aws_iam_role.redshift_copy.arn]
  default_iam_role_arn  = aws_iam_role.redshift_copy.arn
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name      = "${var.name_prefix}-analytics"
  base_capacity       = var.redshift_base_capacity
  publicly_accessible = false
}

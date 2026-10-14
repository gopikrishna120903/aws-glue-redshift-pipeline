variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "rides"
}

variable "alert_email" {
  type        = string
  description = "Email address that receives pipeline failure alerts"
}

variable "schedule_expression" {
  type        = string
  default     = "cron(15 * * * ? *)"
  description = "When the pipeline runs (default: 15 minutes past every hour)"
}

variable "redshift_base_capacity" {
  type    = number
  default = 8 # smallest Redshift Serverless size, in RPUs
}

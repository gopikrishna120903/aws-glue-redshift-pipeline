locals {
  buckets = {
    landing = "${var.name_prefix}-landing-${data.aws_caller_identity.current.account_id}"
    curated = "${var.name_prefix}-curated-${data.aws_caller_identity.current.account_id}"
    scripts = "${var.name_prefix}-scripts-${data.aws_caller_identity.current.account_id}"
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets
  bucket   = each.value
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

# Raw landing files are only needed until processed and audited.
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.this["landing"].id
  rule {
    id     = "expire-raw-trips"
    status = "Enabled"
    filter {
      prefix = "landing/trips/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = 180
    }
  }
}

resource "aws_s3_object" "zones" {
  bucket = aws_s3_bucket.this["landing"].id
  key    = "reference/zones.csv"
  source = "${path.module}/../../data/reference/zones.csv"
}

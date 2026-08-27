resource "aws_s3_bucket" "raw" {
  bucket        = local.raw_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket" "curated" {
  bucket        = local.curated_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_results_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = {
    raw            = aws_s3_bucket.raw.id
    curated        = aws_s3_bucket.curated.id
    athena_results = aws_s3_bucket.athena_results.id
  }

  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = {
    raw            = aws_s3_bucket.raw.id
    curated        = aws_s3_bucket.curated.id
    athena_results = aws_s3_bucket.athena_results.id
  }

  bucket = each.value

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = {
    raw            = aws_s3_bucket.raw.id
    curated        = aws_s3_bucket.curated.id
    athena_results = aws_s3_bucket.athena_results.id
  }

  bucket = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "raw-retention"
    status = "Enabled"

    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 365
    }
  }
}

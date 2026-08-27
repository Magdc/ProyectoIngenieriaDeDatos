resource "aws_athena_workgroup" "analytics" {
  name        = local.athena_workgroup_name
  description = "Athena workgroup for Trend Analyzer historical analytics."

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/queries/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

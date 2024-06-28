#bucket for storage of extracted totesys data (if doesn't exist will create):
resource "aws_s3_bucket" "tf_ingestion_zone" {
  bucket = "blackwater-ingestion-zone"
}

resource "aws_s3_bucket" "tf_processed_zone" {
  bucket = "blackwater-processed-zone"
}

resource "aws_s3_bucket_notification" "processed_trigger" {
  bucket = "blackwater-processed-zone"
  lambda_function {
    lambda_function_arn = aws_lambda_function.load_lambda.arn # add in post
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "last_ran_at"
    filter_suffix       = ".csv"
  }

  depends_on = [
    aws_lambda_permission.load_lambda_s3_trigger
  ]
}

resource "aws_s3_bucket_notification" "ingestion_trigger" {
  bucket = "blackwater-ingestion-zone"
  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "last_ran_at"
    filter_suffix       = ".csv"
  }
  depends_on = [
    aws_lambda_permission.transform_lambda_s3_trigger
  ]
}


#bucket for storage of extracted totesys data (if doesn't exist will create):
resource "aws_s3_bucket" "tf_ingestion_zone" {
  bucket = "blackwater-ingestion-zone"
}

resource "aws_s3_bucket_notification" "ingestion_trigger" {
  bucket = "blackwater-ingestion-zone"
  lambda_function {
    lambda_function_arn = "transform-lambda-arn-to-be-added" # add in post
    events = ["s3:ObjectCreated:*"]
    filter_prefix = "last_ran_at"
    filter_suffix = ".csv"
  }
}

#bucket for storage of extracted totesys data (if doesn't exist will create):
resource "aws_s3_bucket" "tf_processed_zone" {
  bucket = "blackwater-processed-zone"
}

resource "aws_s3_bucket_notification" "processed_trigger" {
  bucket = "blackwater-processed-zone"
  lambda_function {
    lambda_function_arn = "load-lambda-arn-to-be-added" # add in post
    events = ["s3:ObjectCreated:*"]
    filter_prefix = "last_ran_at"
    filter_suffix = ".csv"
  }
}

# #bucket for safe storage of lambda code (if doesn't exist will create):
# resource "aws_s3_bucket" "extract_lambda_storage" {
#   bucket = "blackwater-code-lambda-storage"
# }

# #resource to store lambda code within 'extract lambda storage' bucket:
# resource "aws_s3_object" "extract_lambda_code" {
#   bucket = aws_s3_bucket.extract_lambda_storage.bucket
#   key = "lambda/extract_lambda.zip"
#   source = "${path.module}/../function.zip"
# }

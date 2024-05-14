resource "aws_s3_bucket" "tf_ingestion_zone" {
    bucket = "blackwater-ingestion-zone"
}

resource "aws_s3_bucket" "extract_lambda_storage" {
  bucket = "blackwater-code-lambda-storage"
}

resource "aws_s3_object" "extract_lambda_code" {
  bucket = aws_s3_bucket.extract_lambda_storage.bucket
  key = "lambda/extract_lambda.zip"
  source = "${path.module}/../function.zip"
}
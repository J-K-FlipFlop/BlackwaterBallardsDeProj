#bucket for storage of extracted totesys data (if doesn't exist will create):
resource "aws_s3_bucket" "tf_ingestion_zone" {
    bucket = "blackwater-ingestion-zone"
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
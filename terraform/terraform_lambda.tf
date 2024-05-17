#main lambda function to extract code for storage to s3
resource "aws_lambda_function" "extract_lambda" {
  function_name    = "extract_lambda"
  filename         = "${path.module}/../lambda_extract.zip"
  role             = aws_iam_role.extract_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.extract_lambda_dir_zip.output_base64sha256
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.util_layer.arn]
  timeout          = 45
  memory_size      = 1024
}

#determine source file to extract and its desired output path
# data "archive_file" "extract_lambda_zip" {
#   type        = "zip"
#   source_file = "${path.module}/../src/lambda_extract_function.py"
#   output_path = "${path.module}/../function.zip"
# }

data "archive_file" "extract_lambda_dir_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda/handler.py"
  output_path = "${path.module}/../lambda_extract.zip"
}

#create zip file for the extract lambda dependencies
# data "archive_file" "extract_dependencies_zip" {
#   type        = "zip"
#   source_dir  = "${path.module}/../layers/extract-dependencies/"
#   output_path = "${path.module}/../awswrangler.zip"
# }

#create zip file for the util functions
data "archive_file" "extract_util_functions_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../"
  output_path = "${path.module}/../aws_utils/utils_layer.zip"
  excludes    = [".git", ".pytest_cache", "aws_utils", "layers", "terraform", "test", "venv", ".coverage", ".env", ".gitignore", ".python-version", "awswrangler.zip", "function.zip", "image.png", "lambda_extract.zip", "Makefile", "README-NC.md", "README.md", "requirements.in", "requirements.txt", "test.csv", "src/extract_lambda/handler.py", "src/__pycache__"]
}


resource "aws_lambda_permission" "extract_lambda_eventbridge" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.extract_lambda.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.extract_lambda_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}


#create a lambda awswrangler layer to be used by main lambda functions
# resource "aws_lambda_layer_version" "awswrangler_layer" {
#   layer_name          = "awswrangler_layer"
#   compatible_runtimes = ["python3.11"]
#   filename            = "${path.module}/../awswrangler.zip"
#   source_code_hash    = data.archive_file.extract_dependencies_zip.output_base64sha256
# }

#create a lambda layer for util functions
resource "aws_lambda_layer_version" "util_layer" {
  layer_name          = "util_layer"
  compatible_runtimes = ["python3.11"]
  filename            = "${path.module}/../aws_utils/utils_layer.zip"
  source_code_hash    = data.archive_file.extract_util_functions_zip.output_base64sha256
}

#main lambda function to extract code for storage to s3
resource "aws_lambda_function" "extract_lambda" {
  function_name    = "extract_lambda"
  filename         = "${path.module}/../lambda_extract.zip"
  role             = aws_iam_role.extract_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.extract_lambda_dir_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.awswrangler_layer.arn]
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
  source_dir = "${path.module}/../src/extract_lambda"
  output_path = "${path.module}/../lambda_extract.zip"
  excludes = ["__pycache__"]
}

#create zip file for the extract lambda dependencies
data "archive_file" "extract_dependencies_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../layers/extract-dependencies/"
  output_path = "${path.module}/../awswrangler.zip"
}


resource "aws_lambda_permission" "extract_lambda_eventbridge" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.extract_lambda.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.extract_lambda_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}


#create a lambda awswrangler layer to be used by main lambda functions
resource "aws_lambda_layer_version" "awswrangler_layer" {
  layer_name          = "awswrangler_layer"
  compatible_runtimes = ["python3.11"]
  filename            = "${path.module}/../awswrangler.zip"
  source_code_hash = data.archive_file.extract_dependencies_zip.output_base64sha256
}

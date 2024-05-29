#main lambda function to extract code for storage to s3
resource "aws_lambda_function" "extract_lambda" {
  function_name    = "extract_lambda"
  filename         = "${path.module}/../lambda_extract.zip"
  role             = aws_iam_role.extract_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.extract_lambda_dir_zip.output_base64sha256
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.utility_layer.arn]
  timeout          = 45
  memory_size      = 1024
}

#create zip file for main lambda function
data "archive_file" "extract_lambda_dir_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda/handler.py"
  output_path = "${path.module}/../lambda_extract.zip"
}

#allow lambda to be run on a schedule
resource "aws_lambda_permission" "extract_lambda_eventbridge" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.extract_lambda.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.extract_lambda_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_lambda_permission" "transform_lambda_s3_trigger" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.transform_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.tf_ingestion_zone.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_lambda_permission" "load_lambda_s3_trigger" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.load_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.tf_processed_zone.arn
  source_account = data.aws_caller_identity.current.account_id
}

######## Exract Lambda ########

locals {
  source_files = ["${path.module}/../src/extract_lambda/connection.py", "${path.module}/../src/extract_lambda/credentials_manager.py", "${path.module}/../src/extract_lambda/utils.py"]
}

data "template_file" "t_file" {
  count = length(local.source_files)

  template = file(element(local.source_files, count.index))
}

resource "local_file" "to_temp_dir" {
  count    = length(local.source_files)
  filename = "${path.module}/temp/python/src/extract_lambda/${basename(element(local.source_files, count.index))}"
  content  = element(data.template_file.t_file.*.rendered, count.index)
}

data "archive_file" "archive" {
  type        = "zip"
  output_path = "${path.module}/../aws_utils/utils.zip"
  source_dir  = "${path.module}/temp"

  depends_on = [
    local_file.to_temp_dir,
  ]
}

resource "aws_lambda_layer_version" "utility_layer" {
  layer_name          = "util_layer"
  compatible_runtimes = ["python3.11"]
  filename            = "${path.module}/../aws_utils/utils.zip"
  source_code_hash    = data.archive_file.archive.output_base64sha256
}

######## Transform Lambda ########

resource "aws_lambda_function" "transform_lambda" {
  function_name    = "transform_lambda"
  filename         = "${path.module}/../lambda_transform.zip"
  role             = aws_iam_role.transform_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.transform_lambda_dir_zip.output_base64sha256
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.utility_layer_transform.arn]
  timeout          = 45
  memory_size      = 1024
}

#create zip file for transform lambda function
data "archive_file" "transform_lambda_dir_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/transform_lambda/handler.py"
  output_path = "${path.module}/../lambda_transform.zip"
}

locals {
  source_files_transform = ["${path.module}/../src/transform_lambda/transform_funcs.py", "${path.module}/../src/transform_lambda/utils.py"]
}

data "template_file" "t_file_transform" {
  count    = length(local.source_files_transform)
  template = file(element(local.source_files_transform, count.index))
}

resource "local_file" "to_temp_dir_transform" {
  count    = length(local.source_files_transform)
  filename = "${path.module}/temp_transform/python/src/transform_lambda/${basename(element(local.source_files_transform, count.index))}"
  content  = element(data.template_file.t_file_transform.*.rendered, count.index)
}

data "archive_file" "archive_transform" {
  type        = "zip"
  output_path = "${path.module}/../aws_utils/utils_transform.zip"
  source_dir  = "${path.module}/temp_transform"

  depends_on = [
    local_file.to_temp_dir_transform,
  ]
}

resource "aws_lambda_layer_version" "utility_layer_transform" {
  layer_name          = "util_layer_transform"
  compatible_runtimes = ["python3.11"]
  filename            = "${path.module}/../aws_utils/utils_transform.zip"
  source_code_hash    = data.archive_file.archive_transform.output_base64sha256
}

######## Load Lambda ########

#load lambda, takes data from processed zone and inserts into data warehouse
resource "aws_lambda_function" "load_lambda" {
  function_name    = "load_lambda"
  filename         = "${path.module}/../lambda_load.zip"
  role             = aws_iam_role.load_lambda_role.arn
  handler          = "handler.load_lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.load_lambda_dir_zip.output_base64sha256
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.utility_layer_load.arn]
  timeout          = 45
  memory_size      = 1024
}

#create zip file for load lambda function
data "archive_file" "load_lambda_dir_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/load_lambda/handler.py"
  output_path = "${path.module}/../lambda_load.zip"
}

locals {
  source_files_load = ["${path.module}/../src/load_lambda/connection.py", "${path.module}/../src/load_lambda/credentials_manager.py", "${path.module}/../src/load_lambda/utils.py"]
}

data "template_file" "t_file_load" {
  count = length(local.source_files_load)

  template = file(element(local.source_files_load, count.index))
}

resource "local_file" "to_temp_dir_load" {
  count    = length(local.source_files_load)
  filename = "${path.module}/temp_load/python/src/load_lambda/${basename(element(local.source_files_load, count.index))}"
  content  = element(data.template_file.t_file_load.*.rendered, count.index)
}

data "archive_file" "archive_load" {
  type        = "zip"
  output_path = "${path.module}/../aws_utils/utils_load.zip"
  source_dir  = "${path.module}/temp_load"

  depends_on = [
    local_file.to_temp_dir_load,
  ]
}

resource "aws_lambda_layer_version" "utility_layer_load" {
  layer_name          = "util_layer_load"
  compatible_runtimes = ["python3.11"]
  filename            = "${path.module}/../aws_utils/utils_load.zip"
  source_code_hash    = data.archive_file.archive_load.output_base64sha256
}


#determine source file to extract and its desired output path
# data "archive_file" "extract_lambda_zip" {
#   type        = "zip"
#   source_file = "${path.module}/../src/lambda_extract_function.py"
#   output_path = "${path.module}/../function.zip"
# }

#create zip file for the extract lambda dependencies
# data "archive_file" "extract_dependencies_zip" {
#   type        = "zip"
#   source_dir  = "${path.module}/../layers/extract-dependencies/"
#   output_path = "${path.module}/../awswrangler.zip"
# }

#create zip file for the util functions
# data "archive_file" "extract_util_functions_zip" {
#   type        = "zip"
#   source_content = "${path.module}/../src/extract_lambda/utils.py"
#   source_content_filename = "python/src/extract_lambda/utils.py"
#   output_path = "${path.module}/../aws_utils/utils_layer.zip"
# }

#create a lambda awswrangler layer to be used by main lambda functions
# resource "aws_lambda_layer_version" "awswrangler_layer" {
#   layer_name          = "awswrangler_layer"
#   compatible_runtimes = ["python3.11"]
#   filename            = "${path.module}/../awswrangler.zip"
#   source_code_hash    = data.archive_file.extract_dependencies_zip.output_base64sha256
# }

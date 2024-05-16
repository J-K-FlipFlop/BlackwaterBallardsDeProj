resource "aws_iam_role" "extract_lambda_role" {
  name = "extract-lambda"
  assume_role_policy = data.aws_iam_policy_document.extract_lambda_trust_policy.json
}

data "aws_iam_policy_document" "extract_lambda_trust_policy" {
  statement {
    effect = "Allow"
    principals {
      type = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "write_to_ingestion_zone" {
  statement {
    actions = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.tf_ingestion_zone.arn}/*"]
  }
}

resource "aws_iam_policy" "write_policy_ingestion_zone" {
  name = "write-policy-ingestion-zone"
  policy = data.aws_iam_policy_document.write_to_ingestion_zone.json
}

resource "aws_iam_role_policy_attachment" "attach_write_policy_to_ingestion_zone" {
  role = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.write_policy_ingestion_zone.arn
}

#policy document, policy and role-policy attachment for Cloudwatch /tobewritten/

data "aws_iam_policy_document" "extract_lambda_cloudwatch" {
  statement {
    actions = [ "logs:CreateLogGroup" ]
    resources = [ "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*" ]
  }
  statement {
    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]
    resources = [ "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.extract_lambda.function_name}:*" ]
  }
}

resource "aws_iam_policy" "cloudwatch_policy_extract_lambda" {
  name = "cloudwatch-policy-extract-lambda"
  policy = data.aws_iam_policy_document.extract_lambda_cloudwatch.json
}

resource "aws_iam_role_policy_attachment" "attach_cloudwatch_to_extract_lambda" {
  role = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_policy_extract_lambda.arn
}

data "aws_iam_policy_document" "access_secret_values_lambda" {
  statement {
    sid    = "EnableAnotherAWSAccountToReadTheSecret"
    effect = "Allow"

    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "lambda_access_secrets_policy" {
  name = "lambda_access_secrets_policy"
  policy = data.aws_iam_policy_document.access_secret_values_lambda.json
}

resource "aws_iam_role_policy_attachment" "attach_lambda_access_secrets_to_extract_lambda" {
  role = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.lambda_access_secrets_policy.arn
}

# Extract Lambda IAM code
resource "aws_iam_role" "extract_lambda_role" {
  name               = "extract-lambda"
  assume_role_policy = data.aws_iam_policy_document.extract_lambda_trust_policy.json
}

data "aws_iam_policy_document" "extract_lambda_trust_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "read_write_from_ingestion_zone" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.tf_ingestion_zone.arn}/*"]
  }
}

resource "aws_iam_policy" "read_write_policy_ingestion_zone" {
  name   = "read-write-policy-ingestion-zone"
  policy = data.aws_iam_policy_document.read_write_from_ingestion_zone.json
}

resource "aws_iam_role_policy_attachment" "attach_read_write_policy_to_extract_lambda" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.read_write_policy_ingestion_zone.arn
}

data "aws_iam_policy_document" "extract_lambda_cloudwatch" {
  statement {
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.extract_lambda.function_name}:*"]
  }
}

resource "aws_iam_policy" "cloudwatch_policy_extract_lambda" {
  name   = "cloudwatch-policy-extract-lambda"
  policy = data.aws_iam_policy_document.extract_lambda_cloudwatch.json
}

resource "aws_iam_role_policy_attachment" "attach_cloudwatch_to_extract_lambda" {
  role       = aws_iam_role.extract_lambda_role.name
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
  name   = "lambda_access_secrets_policy"
  policy = data.aws_iam_policy_document.access_secret_values_lambda.json
}

resource "aws_iam_role_policy_attachment" "attach_lambda_access_secrets_to_extract_lambda" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.lambda_access_secrets_policy.arn
}


# Transform Lambda IAM code
resource "aws_iam_role" "transform_lambda_role" {
  name               = "transform-lambda"
  assume_role_policy = data.aws_iam_policy_document.transform_lambda_trust_policy.json
}

data "aws_iam_policy_document" "transform_lambda_trust_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "read_from_ingestion_zone" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = ["${aws_s3_bucket.tf_ingestion_zone.arn}/*"]
  }
}

data "aws_iam_policy_document" "write_to_processed_zone" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.tf_processed_zone.arn}/*"]
  }
}

resource "aws_iam_policy" "read_policy_ingestion_zone" {
  name   = "read-policy-ingestion-zone"
  policy = data.aws_iam_policy_document.read_from_ingestion_zone.json
}

resource "aws_iam_role_policy_attachment" "attach_read_policy_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.read_policy_ingestion_zone.arn
}

resource "aws_iam_policy" "write_policy_processed_zone" {
  name   = "write-policy-processed-zone"
  policy = data.aws_iam_policy_document.write_to_processed_zone.json
}

resource "aws_iam_role_policy_attachment" "attach_write_policy_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.write_policy_processed_zone.arn
}


# data "aws_iam_policy_document" "transform_lambda_cloudwatch" {
#   statement {
#     actions   = ["logs:CreateLogGroup"]
#     resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
#   }
#   statement {
#     actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
#     resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.transform_lambda.function_name}:*"]
#   }
# }

# resource "aws_iam_policy" "cloudwatch_policy_transform_lambda" {
#   name   = "cloudwatch-policy-transform-lambda"
#   policy = data.aws_iam_policy_document.transform_lambda_cloudwatch.json
# }

# resource "aws_iam_role_policy_attachment" "attach_cloudwatch_to_transform_lambda" {
#   role       = aws_iam_role.transform_lambda_role.name
#   policy_arn = aws_iam_policy.cloudwatch_policy_transform_lambda.arn
# }


resource "aws_iam_role_policy_attachment" "attach_lambda_access_secrets_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.lambda_access_secrets_policy.arn
}


# Load Lambda IAM code
resource "aws_iam_role" "load_lambda_role" {
  name               = "load-lambda"
  assume_role_policy = data.aws_iam_policy_document.load_lambda_trust_policy.json
}

data "aws_iam_policy_document" "load_lambda_trust_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "read_from_processed_zone" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = ["${aws_s3_bucket.tf_processed_zone.arn}/*"]
  }
}

resource "aws_iam_policy" "read_policy_processed_zone" {
  name   = "read-policy-processed-zone"
  policy = data.aws_iam_policy_document.read_from_processed_zone.json
}

resource "aws_iam_role_policy_attachment" "attach_read_policy_to_load_lambda" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.read_policy_ingestion_zone.arn
}



# data "aws_iam_policy_document" "load_lambda_cloudwatch" {
#   statement {
#     actions   = ["logs:CreateLogGroup"]
#     resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
#   }
#   statement {
#     actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
#     resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.load_lambda.function_name}:*"]
#   }
# }

# resource "aws_iam_policy" "cloudwatch_policy_load_lambda" {
#   name   = "cloudwatch-policy-load-lambda"
#   policy = data.aws_iam_policy_document.load_lambda_cloudwatch.json
# }

# resource "aws_iam_role_policy_attachment" "attach_cloudwatch_to_load_lambda" {
#   role       = aws_iam_role.load_lambda_role.name
#   policy_arn = aws_iam_policy.cloudwatch_policy_load_lambda.arn
# }


resource "aws_iam_role_policy_attachment" "attach_lambda_access_secrets_to_load_lambda" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.lambda_access_secrets_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_read_policy_to_load_lambda" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.read_policy_ingestion_zone.arn
}
resource "aws_cloudwatch_log_metric_filter" "extract_lambda_errors" {
  name           = "ExtractLambdaErrors"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/${aws_lambda_function.extract_lambda.function_name}"
  metric_transformation {
    name      = "ExtractLambdaErrors"
    namespace = "Errors"
    value     = "1"
  }
}


resource "aws_cloudwatch_metric_alarm" "extract_lambda_errors_alarm" {
  alarm_name          = "AlertExtractLambdaErrors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ExtractLambdaErrors"
  namespace           = "Errors"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Identifies a errors when the extract lambda function runs"
  alarm_actions       = [aws_sns_topic.extract_lambda_errors.arn]
  treat_missing_data  = "notBreaching"
}


resource "aws_sns_topic" "extract_lambda_errors" {
  name = "extract_lambda_errors"

}

resource "aws_sns_topic_subscription" "send_extract_lambda_errors_richard" {
  protocol  = "email"
  endpoint  = "rpwilding@proton.me"
  topic_arn = aws_sns_topic.extract_lambda_errors.arn
}

resource "aws_sns_topic_subscription" "send_extract_lambda_errors_mike" {
  protocol  = "email"
  endpoint  = "mikey.5881@gmail.com"
  topic_arn = aws_sns_topic.extract_lambda_errors.arn
}

## Transform lambda filtered metrics and alarms

resource "aws_cloudwatch_log_group" "transform_lamba_logg" {
  name = aws_lambda_function.transform_lambda.function_name
}

resource "aws_cloudwatch_log_metric_filter" "transform_lambda_errors" {
  name           = "TransformLambdaErrors"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/${aws_cloudwatch_log_group.transform_lamba_logg.name}"
  metric_transformation {
    name      = "TransformLambdaErrors"
    namespace = "Transform Errors"
    value     = "1"
  }
}
resource "aws_cloudwatch_metric_alarm" "transform_lambda_errors_alarm" {
  alarm_name          = "AlertTransformLambdaErrors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "TransformLambdaErrors"
  namespace           = "Errors"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Identifies any errors when the transform lambda function runs"
  alarm_actions       = [aws_sns_topic.transform_lambda_errors.arn]
  treat_missing_data  = "notBreaching"
}
resource "aws_sns_topic" "transform_lambda_errors" {
  name = "transform_lambda_errors"
}
resource "aws_sns_topic_subscription" "send_transfer_lambda_errors_mike" {
  protocol  = "email"
  endpoint  = "mikey.5881@gmail.com"
  topic_arn = aws_sns_topic.transform_lambda_errors.arn
}
resource "aws_sns_topic_subscription" "send_transform_lambda_errors_richard" {
  protocol  = "email"
  endpoint  = "rpwilding@proton.me"
  topic_arn = aws_sns_topic.transform_lambda_errors.arn
}
# Load lambda filtered metrics and alarms

resource "aws_cloudwatch_log_group" "load_lamba_logg" {
  name = aws_lambda_function.load_lambda.function_name
}

resource "aws_cloudwatch_log_metric_filter" "load_lambda_errors" {
  name           = "LoadLambdaErrors"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/${aws_cloudwatch_log_group.load_lamba_logg.name}"
  metric_transformation {
    name      = "LoadLambdaErrors"
    namespace = "Load Errors"
    value     = "1"
  }
}
resource "aws_cloudwatch_metric_alarm" "load_lambda_errors_alarm" {
  alarm_name          = "AlertLoadLambdaErrors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "LoadLambdaErrors"
  namespace           = "Errors"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Identifies any errors when the load lambda function runs"
  alarm_actions       = [aws_sns_topic.load_lambda_errors.arn]
  treat_missing_data  = "notBreaching"
}
resource "aws_sns_topic" "load_lambda_errors" {
  name = "load_lambda_errors"
}
resource "aws_sns_topic_subscription" "send_load_lambda_errors_mike" {
  protocol  = "email"
  endpoint  = "mikey.5881@gmail.com"
  topic_arn = aws_sns_topic.load_lambda_errors.arn
}
resource "aws_sns_topic_subscription" "send_load_lambda_errors_richard" {
  protocol  = "email"
  endpoint  = "rpwilding@proton.me"
  topic_arn = aws_sns_topic.load_lambda_errors.arn
}

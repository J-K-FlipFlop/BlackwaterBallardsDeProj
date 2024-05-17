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
  period              = "30"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Identifies a errors when the extract lambda function runs"
  alarm_actions       = [aws_sns_topic.extract_lambda_errors.arn]
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

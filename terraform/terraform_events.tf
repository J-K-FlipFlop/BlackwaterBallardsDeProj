#sets up scheduler to be invoked in 5 minute increments
resource "aws_cloudwatch_event_rule" "extract_lambda_scheduler" {
  name                = "extract_lambda_5_mins"
  description         = "triggers the extract lambda every 5 minutes"
  schedule_expression = "rate(5 minutes)"
}
#designates target lambda function to be invoked by scheduler
resource "aws_cloudwatch_event_target" "extract_lambda_target" {
  arn  = aws_lambda_function.extract_lambda.arn
  rule = aws_cloudwatch_event_rule.extract_lambda_scheduler.id
}

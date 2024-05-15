resource "aws_cloudwatch_event_rule" "scheduler" {
    name = "FiveMinuteLambda"
    description = "grabs lambda function every 5 minutes"
    schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "scheduler_targeter" {
  rule = aws_cloudwatch_event_rule.scheduler.name
  target_id = "extract_lambda"
  arn = aws_lambda_function.extract_lambda.arn
}
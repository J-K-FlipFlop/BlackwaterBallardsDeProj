# resource "aws_lambda_function" "fake_lambda_for_testing" {
#   function_name = "fake_lambda"
#   role = aws_iam_role.extract_lambda_role.arn
#   filename = ""
# }
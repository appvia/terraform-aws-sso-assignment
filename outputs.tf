output "config_dynamodb_table_arn" {
  description = "ARN of the DynamoDB table storing group configurations"
  value       = aws_dynamodb_table.config.arn
}

output "config_dynamodb_table_name" {
  description = "Name of the DynamoDB table storing group configurations"
  value       = aws_dynamodb_table.config.name
}

output "eventbridge_invoke_role_arn" {
  description = "ARN of EventBridge roles for account creation and cron schedule"
  value       = aws_iam_role.eventbridge_invoke.arn
}

output "eventbridge_rule_arns" {
  description = "ARNs of EventBridge rules for account creation and cron schedule"
  value = {
    account_creation = try(aws_cloudwatch_event_rule.account_creation[0].arn, null)
    cron_schedule    = aws_cloudwatch_event_rule.cron_schedule.arn
    config_update    = try(aws_pipes_pipe.config_update[0].arn, null)
  }
}

output "eventbridge_rule_names" {
  description = "Names of EventBridge rules for account creation and cron schedule"
  value = {
    account_creation = try(aws_cloudwatch_event_rule.account_creation[0].name, null)
    cron_schedule    = aws_cloudwatch_event_rule.cron_schedule.name
    config_update    = try(aws_pipes_pipe.config_update[0].name, null)
  }
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function for SSO group assignment"
  value       = module.lambda.lambda_function_arn
}

output "lambda_function_name" {
  description = "Name of the Lambda function for SSO group assignment"
  value       = module.lambda.lambda_function_name
}

output "lambda_policy_json" {
  description = "IAM policy document (JSON) attached to the Lambda role via policy_json"
  value       = data.aws_iam_policy_document.lambda_policy.json
}

output "step_function_arn" {
  description = "ARN of the Step Function state machine orchestrating SSO assignments"
  value       = aws_sfn_state_machine.main.arn
}

output "tracking_dynamodb_table_arn" {
  description = "ARN of the DynamoDB table tracking managed SSO assignments"
  value       = aws_dynamodb_table.assignments_tracking.arn
}

output "tracking_dynamodb_table_name" {
  description = "Name of the DynamoDB table tracking managed SSO assignments"
  value       = aws_dynamodb_table.assignments_tracking.name
}

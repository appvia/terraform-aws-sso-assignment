output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table storing group configurations"
  value       = aws_dynamodb_table.config.arn
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table storing group configurations"
  value       = aws_dynamodb_table.config.name
}

output "assignments_tracking_table_arn" {
  description = "ARN of the DynamoDB table tracking managed SSO assignments"
  value       = aws_dynamodb_table.assignments_tracking.arn
}

output "assignments_tracking_table_name" {
  description = "Name of the DynamoDB table tracking managed SSO assignments"
  value       = aws_dynamodb_table.assignments_tracking.name
}

output "eventbridge_rule_arns" {
  description = "ARNs of EventBridge rules for account creation and cron schedule"
  value = {
    account_creation = aws_cloudwatch_event_rule.account_creation.arn
    cron_schedule    = aws_cloudwatch_event_rule.cron_schedule.arn
    config_update    = try(aws_pipes_pipe.config_update[0].arn, null)
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

output "step_function_arn" {
  description = "ARN of the Step Function state machine orchestrating SSO assignments"
  value       = aws_sfn_state_machine.main.arn
}
